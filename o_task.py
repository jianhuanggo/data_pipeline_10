from _orchestration._actor_model import tasks

def run():
    # from uuid import uuid4

    tasks.add_task(task_id=3, task={
        "task_type": "python",
        "task_details": [
      {
        "function_name": "history_load_run",
        "function_parameters": [ ["2025-01-12"], ["2025-01-13"], ["2025-01-14"], ["2025-01-15"] ]
      }
    ]
    }
    )

    tasks._display_tasks(task_id=1)
    tasks._display_tasks(task_id=2)
    tasks._display_tasks(task_id=3)

    # tasks._delete_task(task_id=2)
    # tasks.delete_all_task()
    #
    # tasks._display_tasks(task_id=1)
    # tasks._display_tasks(task_id=2)
    # tasks._display_tasks(task_id=3)
    tasks.display_all_tasks()
    tasks.run_task(task_id=3)


if __name__ == '__main__':
    run()