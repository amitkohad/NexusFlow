"""Package executor role closure, deployment identity and capacity validation."""

from __future__ import annotations

from typing import Any

import pytest
from contracts import ExecutorPool
from nexusflow_common.errors import ConfigurationError
from nexusflow_common.executor import load_executor_settings
from temporalio.exceptions import ApplicationError
from temporalio.worker import PollerBehaviorSimpleMaximum
from workflow_executor.boundary import validate_execution_content
from workflow_executor.host import load_executor_package, registrations_for_pool, worker_options
from workflow_sdk.packages import resolve_binding

from tests.package_fixtures import source_package, start_request, synthetic_release


def pool(role: str = "mixed", **changes: Any) -> ExecutorPool:
    values = {
        "pool_id": "customer-adjustment-" + role,
        "package_id": "customer-adjustment",
        "package_release_id": "customer-adjustment-0.1.0",
        "worker_deployment_name": "nexusflow-customer-adjustment",
        "build_id": "customer-adjustment-0.1.0",
        "role": role,
        "queue_bindings": {
            "workflow": "customer-adjustment-tq",
            "activities": "customer-adjustment-tq",
        },
    }
    values.update(changes)
    return ExecutorPool.model_validate(values)


@pytest.mark.parametrize(
    "role, workflows, activities", [("mixed", 1, 6), ("workflow", 1, 0), ("activity", 0, 6)]
)
def test_role_replicas_register_complete_same_release_closure(
    role: str, workflows: int, activities: int
) -> None:
    package = source_package()
    config = load_executor_settings(pool(role), {})
    grouped = registrations_for_pool(package, config)
    assert list(grouped) == ["customer-adjustment-tq"]
    assert len(grouped["customer-adjustment-tq"]["workflows"]) == workflows
    assert len(grouped["customer-adjustment-tq"]["activities"]) == activities


def test_mixed_process_can_own_distinct_workflow_and_activity_queues() -> None:
    settings = load_executor_settings(
        pool(queue_bindings={"workflow": "workflow-tq", "activities": "activity-tq"}), {}
    )
    grouped = registrations_for_pool(source_package(), settings)
    assert len(grouped["workflow-tq"]["workflows"]) == 1
    assert len(grouped["activity-tq"]["activities"]) == 6


@pytest.mark.parametrize(
    "changes",
    [
        {"package_id": "another-package"},
        {"build_id": "another-build"},
        {"worker_deployment_name": "another-deployment"},
        {"queue_bindings": {"workflow": "customer-adjustment-tq"}},
        {"desired_state": "draining"},
        {"desired_state": "retired"},
    ],
)
def test_pool_must_match_manifest_and_serving_role(changes: dict[str, Any]) -> None:
    config = load_executor_settings(pool(**changes), {})
    with pytest.raises(ConfigurationError):
        registrations_for_pool(source_package(), config)


def test_capacity_maps_to_modern_sdk_separate_slots_pollers_and_rates() -> None:
    config = load_executor_settings(
        pool(),
        {
            "NEXUSFLOW_WORKFLOW_TASK_SLOTS": "3",
            "NEXUSFLOW_ACTIVITY_TASK_SLOTS": "7",
            "NEXUSFLOW_WORKFLOW_TASK_POLLERS": "2",
            "NEXUSFLOW_ACTIVITY_TASK_POLLERS": "4",
            "NEXUSFLOW_MAX_ACTIVITIES_PER_SECOND": "8.5",
            "NEXUSFLOW_MAX_TASK_QUEUE_ACTIVITIES_PER_SECOND": "10",
        },
    )
    options = worker_options(config)
    assert options["max_concurrent_workflow_tasks"] == 3
    assert options["max_concurrent_activities"] == 7
    assert options["workflow_task_poller_behavior"] == PollerBehaviorSimpleMaximum(2)
    assert options["activity_task_poller_behavior"] == PollerBehaviorSimpleMaximum(4)
    assert options["max_activities_per_second"] == 8.5
    deployment = options["deployment_config"]
    assert deployment.use_worker_versioning is True
    assert deployment.version.deployment_name == "nexusflow-customer-adjustment"
    assert deployment.version.build_id == "customer-adjustment-0.1.0"
    assert "build_id" not in options and "use_worker_versioning" not in options


@pytest.mark.parametrize(
    "environment",
    [
        {"NEXUSFLOW_ACTIVITY_TASK_SLOTS": "0"},
        {"NEXUSFLOW_WORKFLOW_TASK_SLOTS": "true"},
        {"NEXUSFLOW_WORKFLOW_TASK_POLLERS": "101"},
        {"NEXUSFLOW_MAX_ACTIVITIES_PER_SECOND": "NaN"},
        {"NEXUSFLOW_MAX_ACTIVITIES_PER_SECOND": "0"},
        {"NEXUSFLOW_EXECUTOR_INSTANCE_ID": "untrusted:identity"},
        {"NEXUSFLOW_PROBE_PORT": "0"},
        {"NEXUSFLOW_PROBE_HOST": "example.com"},
        {"TEMPORAL_TASK_QUEUE": "external-worker-tq"},
        {"TEMPORAL_NAMESPACE": "another"},
        {"NEXUSFLOW_ENVIRONMENT": "prod"},
    ],
)
def test_invalid_capacity_or_connection_rejected_before_polling(
    environment: dict[str, str],
) -> None:
    with pytest.raises(ConfigurationError):
        load_executor_settings(pool(), environment)


def test_instances_use_unique_identity_on_stable_shared_queues() -> None:
    first = load_executor_settings(pool(), {})
    second = load_executor_settings(pool(), {})
    assert first.identity != second.identity
    assert first.pool.queue_bindings == second.pool.queue_bindings
    assert first.pool.build_id in first.identity
    assert first.pool.pool_id in first.identity


def test_untrusted_package_name_cannot_be_an_import_path() -> None:
    with pytest.raises(ConfigurationError):
        load_executor_package("os:system", development_source=True)


def test_pinned_boundary_requires_original_build_and_exact_retained_definition() -> None:
    package = source_package()
    request = start_request(package)
    validate_execution_content(
        package.manifest, pool(), request.release_binding, "PackageWorkflowV1"
    )
    with pytest.raises(ApplicationError) as error:
        validate_execution_content(
            package.manifest,
            pool(build_id="new-build"),
            request.release_binding,
            "PackageWorkflowV1",
        )
    assert error.value.non_retryable
    changed = package.manifest.model_copy(
        update={
            "definitions": (package.manifest.definitions[0].model_copy(update={"version": "2.0"}),)
        }
    )
    with pytest.raises(ApplicationError):
        validate_execution_content(changed, pool(), request.release_binding, "PackageWorkflowV1")


def test_auto_upgrade_boundary_accepts_later_build_with_retained_exact_closure() -> None:
    package = source_package()
    original = package.manifest.model_copy(
        update={
            "workflow_registrations": (
                package.manifest.workflow_registrations[0].model_copy(
                    update={
                        "name": "PackageAutoUpgradeWorkflowV1",
                        "entrypoint": "workflow_sdk.runtime:PackageAutoUpgradeWorkflowV1",
                        "versioning_behavior": "auto_upgrade",
                    }
                ),
            ),
            "definitions": (
                package.manifest.definitions[0].model_copy(
                    update={"runtime_workflow_type": "PackageAutoUpgradeWorkflowV1"}
                ),
            ),
        }
    )
    binding = resolve_binding(original, synthetic_release(original), original.definitions[0])
    upgraded = original.model_copy(update={"build_id": "new-build", "package_version": "0.2.0"})
    assert "new-build" not in binding.eligible_build_ids
    validate_execution_content(
        upgraded, pool(build_id="new-build"), binding, "PackageAutoUpgradeWorkflowV1"
    )
    incompatible = upgraded.model_copy(
        update={
            "activity_registrations": tuple(
                item
                for item in upgraded.activity_registrations
                if item.capability != "post_adjustment"
            )
        }
    )
    with pytest.raises(ApplicationError):
        validate_execution_content(
            incompatible, pool(build_id="new-build"), binding, "PackageAutoUpgradeWorkflowV1"
        )


def test_upgrade_boundary_rejects_changed_stable_activity_queues() -> None:
    package = source_package()
    request = start_request(package)
    with pytest.raises(ApplicationError):
        validate_execution_content(
            package.manifest,
            pool(queue_bindings={"workflow": "customer-adjustment-tq", "activities": "other-tq"}),
            request.release_binding,
            "PackageWorkflowV1",
        )


@pytest.mark.parametrize("changes", [{"manifest_hash": "a" * 64}, {"package_version": "0.2.0"}])
def test_same_build_requires_exact_manifest_and_package_version(changes: dict[str, Any]) -> None:
    package = source_package()
    request = start_request(package)
    corrupted = request.release_binding.model_copy(update=changes)
    with pytest.raises(ApplicationError) as failure:
        validate_execution_content(package.manifest, pool(), corrupted, "PackageWorkflowV1")
    assert failure.value.type == "PackageCompatibilityError"
    assert failure.value.non_retryable
