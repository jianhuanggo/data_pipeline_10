


def main():
    from _orchestration._actor_model import worker
    from _orchestration._actor_model import actor_common as _actor_common_

    # print(_actor_common_.query_object("workers_metadata", recurse_flg=True))
    # exit(0)

    # worker.register_worker(116)
    # worker.register_worker(117)
    # worker.register_worker(118)
    # worker.register_worker(119)
    worker.display_all_workers()
    worker.remove_all_workers()


    # worker.remove_worker(25)
    # worker.remove_worker(28)
    # worker.remove_worker(29)

    # _actor_common_._delete("workers_metadata")
    # _actor_common_._delete("workers_tasks")
    # exit(0)

    worker.display_all_workers()
    exit(0)


    worker.run()




if __name__ == "__main__":
    main()