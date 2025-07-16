def run_dispatcher():
    from _orchestration._actor_model import dispatcher
    dispatcher.run()


if __name__ == '__main__':
    run_dispatcher()