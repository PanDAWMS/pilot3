"""Kubernetes-native payload executor for running jobs in a separate pod container."""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, TextIO

from pilot.info import JobData
from pilot.util.config import config


logger = logging.getLogger(__name__)

DEFAULT_SHARED_MOUNT = "/pilot-shared"  # ensure this matches Pod spec


class Executor:
    """Kubernetes-native payload executor.

    Runs a job payload in a separate container within the same pod as the
    pilot, communicating via a shared volume mount. The payload container
    image is patched onto the pod at runtime ("late-bind"), and completion
    is signalled by an exit file written into the shared volume.

    The ``kubernetes`` package is imported lazily inside ``__init__`` so that
    this module can be safely imported on grid worker nodes where the package
    is not installed. An ``ImportError`` is raised at instantiation time (not
    at module import time) if the package is missing.
    """

    def __init__(self, args: object, job: JobData, out: TextIO, err: TextIO, traces: Any):
        """Initialize the Kubernetes executor.

        Imports the ``kubernetes`` package lazily, then loads Kubernetes
        configuration (in-cluster first, then kubeconfig) and sets up the
        CoreV1Api client used for pod operations.

        The constructor arguments beyond ``args`` are retained on the instance
        for use by subclasses or future extensions (e.g. streaming stdout/stderr
        directly to ``out``/``err`` during execution).

        Args:
            args: Pilot arguments object containing queue name, queuedata, etc.
            job: PanDA job object describing the payload to execute.
            out: File object for capturing payload stdout.
            err: File object for capturing payload stderr.
            traces: Internal pilot state traces object.

        Raises:
            ImportError: If the ``kubernetes`` package is not installed.
        """
        try:
            from kubernetes import client, config as k8s_config           # pylint: disable=import-outside-toplevel
            from kubernetes.config.config_exception import ConfigException  # pylint: disable=import-outside-toplevel
        except ImportError as exc:
            raise ImportError(
                "The 'kubernetes' package is required for the Kubernetes executor "
                "but is not installed on this node. Install it with: "
                "pip install kubernetes"
            ) from exc

        self.pod_name = os.environ.get("POD_NAME", "payload_pod")
        self.namespace = os.environ.get("POD_NAMESPACE", "default")
        self.payload_container_name = os.environ.get("POD_PAYLOAD_NAME", "payload")

        # Retained for subclass use and future extensions (stdout/stderr streaming,
        # abort signalling, trace updates). Part of the standard executor interface
        # shared across all executor types; single-underscore prefix avoids the
        # unused-private-member warning while keeping them non-public.
        self._args = args
        self._job = job
        self._out = out
        self._err = err
        self._traces = traces

        # Load in-cluster config first; fall back to kubeconfig; warn if neither works.
        try:
            k8s_config.load_incluster_config()
        except ConfigException:
            try:
                k8s_config.load_kube_config()
            except ConfigException:
                logger.warning("No kubernetes config found; k8s operations will fail.")
        self.core_v1 = client.CoreV1Api()
        # Keep a reference to the client module for use in _set_payload_image and cleanup.
        self._k8s_client = client

    def prepare(self, job: JobData, workdir: str = DEFAULT_SHARED_MOUNT) -> None:
        """Stage input files into the shared volume and create the startup wrapper skeleton.

        This method should reuse existing staging logic, targeting ``workdir``
        inside the shared mount (e.g. ``os.path.join(workdir, job.jobid)``).

        Args:
            job: PanDA job object describing the payload to execute.
            workdir: Absolute path to the shared volume mount used for
                inter-container communication. Defaults to
                ``DEFAULT_SHARED_MOUNT``.
        """
        # TODO: reuse existing staging logic targeting workdir inside shared mount,
        # e.g. stage_input_files(job, dst=os.path.join(workdir, job.jobid))
        _ = (job, workdir)  # not-yet-implemented stub; avoids unused-argument warning

    def _set_payload_image(self, image: str) -> None:
        """Patch the pod to update the payload container image.

        Uses a strategic-merge patch against the running pod so that the
        payload container is restarted with the specified image. This is
        the "late-bind" mechanism that allows the pilot to select the
        user-supplied container image at runtime.

        Args:
            image: Fully-qualified container image reference to apply
                (e.g. ``"registry.example.com/user/image:tag"``).

        Raises:
            kubernetes.client.exceptions.ApiException: If the Kubernetes API
                call fails (e.g. pod not found, RBAC denied).
        """
        body = {
            "spec": {
                "containers": [
                    {"name": self.payload_container_name, "image": image}
                ]
            }
        }
        try:
            self.core_v1.patch_namespaced_pod(
                name=self.pod_name, namespace=self.namespace, body=body
            )
        except self._k8s_client.exceptions.ApiException as exc:
            logger.exception("Failed to patch pod: %s", exc)
            raise

    def _write_startup_wrapper(self, workdir: str, job: JobData) -> None:
        """Write the payload startup wrapper script into the shared mount.

        The script is written atomically (via a ``.tmp`` staging file and
        ``os.replace``) to avoid the payload container reading a partially
        written file. The wrapper sources an optional environment file,
        executes the job command line, records the exit code and end
        timestamp into ``exit.json``, and propagates the exit code.

        Args:
            workdir: Absolute path to the shared volume mount directory.
            job: PanDA job object; ``job.transformation`` is used as the
                command line to execute inside the payload container.
        """
        wrapper_path = os.path.join(workdir, "payload_startup.sh")
        cmdline = getattr(job, "transformation", "") or ""
        wrapper = f"""#!/bin/sh
set -e
# source env if present
[ -f {workdir}/payload_env.sh ] && . {workdir}/payload_env.sh
# run the user command
{cmdline}
rc=$?
# collect simple metadata
echo '{{"exit_code":'\"$rc\"', "end_time":"'$(date -Is)'"}}' > {workdir}/exit.tmp
mv {workdir}/exit.tmp {workdir}/exit.json
exit $rc
"""
        with open(wrapper_path + ".tmp", "w", encoding="utf-8") as fh:
            fh.write(wrapper)
        os.chmod(wrapper_path + ".tmp", 0o755)
        os.replace(wrapper_path + ".tmp", wrapper_path)

    def run(self, job: JobData, workdir: str = DEFAULT_SHARED_MOUNT, timeout: float or None = None) -> dict:
        """Execute the payload and return its result.

        Orchestrates the full execution lifecycle:

        1. Calls :meth:`prepare` to stage input files.
        2. Writes the startup wrapper script via :meth:`_write_startup_wrapper`.
        3. Patches the pod's payload container image via :meth:`_set_payload_image`.
        4. Polls for the ``exit.json`` sentinel file written by the wrapper.

        Args:
            job: PanDA job object. Must have a ``container_image`` attribute
                specifying the user-supplied container image to late-bind.
            workdir: Absolute path to the shared volume mount directory.
                Defaults to ``DEFAULT_SHARED_MOUNT``.
            timeout: Maximum number of seconds to wait for the payload to
                finish. ``None`` means wait indefinitely.

        Returns:
            A dict with the following keys:

            - ``"exit_code"`` (int): The payload's exit code.
            - ``"meta"`` (dict): The full contents of ``exit.json``,
              including at minimum ``exit_code`` and ``end_time``.

        Raises:
            RuntimeError: If ``job.container_image`` is not set.
            TimeoutError: If ``timeout`` is exceeded before ``exit.json``
                appears.
            json.JSONDecodeError: If ``exit.json`` cannot be parsed.
            OSError: If ``exit.json`` cannot be opened after appearing on disk.
        """
        self.prepare(job, workdir=workdir)

        user_image = getattr(job, "container_image", None)
        if not user_image:
            raise RuntimeError("Job missing container_image for k8s late-bind")

        self._write_startup_wrapper(workdir, job)
        self._set_payload_image(user_image)

        exit_json = os.path.join(workdir, "exit.json")
        start = time.time()
        poll_interval = 2
        while True:
            if os.path.exists(exit_json):
                with open(exit_json, "r", encoding="utf-8") as fh:
                    data = json.load(fh)
                return {"exit_code": int(data.get("exit_code", 1)), "meta": data}
            if timeout and (time.time() - start) > timeout:
                raise TimeoutError("Timeout waiting for payload exit file")
            time.sleep(poll_interval)

    def cleanup(self, job: JobData, workdir: str = DEFAULT_SHARED_MOUNT) -> None:  # pylint: disable=unused-argument
        """Clean up after payload execution.

        Resets the payload container image to a lightweight placeholder
        (forcing a container restart that terminates any remaining payload
        processes), then removes the sentinel and wrapper files from the
        shared volume.

        The placeholder image is read from ``config.k8s_payload_placeholder_image``
        and defaults to ``"busybox:latest"`` if not configured.

        Args:
            job: PanDA job object (reserved for future use, e.g. job-specific
                cleanup steps).
            workdir: Absolute path to the shared volume mount directory.
                Defaults to ``DEFAULT_SHARED_MOUNT``.
        """
        placeholder = getattr(config, "k8s_payload_placeholder_image", "busybox:latest")
        try:
            self._set_payload_image(placeholder)
        except self._k8s_client.exceptions.ApiException:
            logger.warning("Failed to reset payload image during cleanup.")

        try:
            for name in ("exit.json", "payload_startup.sh"):
                p = os.path.join(workdir, name)
                if os.path.exists(p):
                    os.remove(p)
        except OSError:
            logger.exception("Failed cleanup of workdir")
