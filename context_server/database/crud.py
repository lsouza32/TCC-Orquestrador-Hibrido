# Faz a conexão com o banco de dados e importa funções CRUD específicas

from context_server.database.base import get_db_connection, init_db

from context_server.database.nodes_crud import (
    register_or_update_node_static_info,
    update_node_status,
    update_node_resources,
    get_all_nodes,
    get_active_nodes,
    get_node_details,
    reserve_node_resources,
    release_node_resources,
)

from context_server.database.tasks_crud import (
    create_task,
    update_task_allocation,
    update_task_planning,
    update_task_status,
    save_task_result,
    get_task_result,
    get_all_task_results,
    get_user_tasks_with_results,
    get_task_with_requirements,
    get_task_by_id,
    get_tasks_in_queue,
    get_recent_task_results_for_calibration,
    get_completed_task_counts_by_node_for_image,
    get_stale_docker_images_for_node,
    get_protected_docker_images_for_node,
    get_docker_images_catalog,
    find_catalog_candidates_for_libraries,
    get_telemetry_summary_for_libraries,
    get_task_result_samples_for_libraries,
    get_docker_image_success_stats_for_libraries,
    get_resource_ceiling_signals_for_libraries,
    persist_effective_k8s_dispatch,
    enqueue_task_for_reallocation,
)

__all__ = [
    # base
    "get_db_connection", "init_db",
    # nodes
    "register_or_update_node_static_info", "update_node_status", "update_node_resources",
    "get_all_nodes", "get_active_nodes", "get_node_details", "reserve_node_resources", "release_node_resources",
    # tasks
    "create_task", "update_task_allocation", "update_task_status",
    "update_task_planning",
    "save_task_result", "get_task_result", "get_all_task_results", "get_user_tasks_with_results",
    "get_task_with_requirements", "get_task_by_id", "get_tasks_in_queue",
    "get_recent_task_results_for_calibration",
    "get_completed_task_counts_by_node_for_image",
    "get_stale_docker_images_for_node",
    "get_protected_docker_images_for_node",
    "get_docker_images_catalog", "find_catalog_candidates_for_libraries", "get_telemetry_summary_for_libraries",
    "get_task_result_samples_for_libraries",
    "get_docker_image_success_stats_for_libraries",
    "get_resource_ceiling_signals_for_libraries",
    "persist_effective_k8s_dispatch",
    "enqueue_task_for_reallocation",
]
