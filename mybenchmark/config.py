import os

config = {
    'result_dir': os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), "results")
}


def generate_default_output_dir(setting):
    run_output_dir = os.path.join(
        config['result_dir'],
        setting['experiment_id'],
        "{}roxie_{}_{}_{}_{}queries_{}sec".format(
            len(setting['hpcc_cluster'].get_roxie_cluster()),
            setting['arrival_type'],
            setting['distribution']['type'],
            setting['application'],
            setting['num_queries'],
            setting['period']
        )
     )
    return run_output_dir