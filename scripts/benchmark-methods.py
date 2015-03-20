#!/usr/bin/env python
"""

"""

import subprocess as sub
import csv

def time_it(program):
    command="/usr/bin/env time -f \"%E\" " +  program + " 2>&1 | tail -1"
    try:
        p = sub.check_output(command, shell=True, stderr=sub.STDOUT)
        out = p.communicate()
    except sub.CalledProcessError, err:
        print "The command used to launch the failed subprocess is was [%s]." % err.cmd
        print "The output of the command was [%s]" % err.output
        raise
    return out

def generate_matrix(project_root, out_path, n_rows, n_cols, frac, block_size, master, spark_home):
    gen_mat_args = (project_root, out_path, n_rows, n_cols, frac, block_size, master, spark_home)
    gen_mat_cmd = "%s/scripts/gen-matrix.sh %s %s %s %s %s %s %s" % gen_mat_args
    try:
        sub.call(gen_mat_cmd, shell=True)
    except OSError:
        print "Oops! OS Error. Could not run the command:\n %s" % gen_mat_cmd

def spark_factorize_and_time(project_root, in_path, out_u, out_s, out_v, master, sparkHome):
    svd_args = (project_root, in_path, out_u, out_s, out_v, master, sparkHome)
    svd_cmd = "%s/scripts/spark-svd.sh %s %s %s %s %s %s" % svd_args
    try:
        elapsed_time = time_it(svd_cmd)
    except OSError:
        print "Oops! OS Error. Could not run the command:\n %s" % svd_cmd
    return elapsed_time

def lanczos_factorize_and_time(project_root, in_path, out_path, n_rows, n_cols, rank):
    lan_args = (project_root, in_path, out_path, n_rows, n_cols, rank)
    lan_cmd = "{%s/scripts/lanczos-svd.sh %s %s %s %s %s 2> spark.logs; }" % lan_args
    try:
        elapsed_time = time_it(lan_cmd)
    except OSError:
        print "Oops! OS Error. Could not run the command:\n %s" % lan_cmd
    return elapsed_time

def stochastic_factorize_and_time(project_root, in_path, out_path, rank):
    stoch_args = (project_root, in_path, out_path, rank)
    stoch_cmd = "{%s/scripts/lanczos-svd.sh %s %s %s 2> spark.logs; }" % stoch_args
    try:
        elapsed_time = time_it(stoch_cmd)
    except OSError:
        print "Oops! OS Error. Could not run the command:\n %s" % stoch_cmd
    return elapsed_time


def process_one_param_set(n_rows, n_cols, frac, lan_rank, stoch_rank, block_size, master, spark_home, project_home,
                          hdfs_root, csv_writer):
    # setup paths for these matrices
    gen_mat_path=hdfs_root + "/gen-matrix-nrow%s-ncols%s-sp%s" % (n_rows, n_cols, frac)
    out_u=hdfs_root + "/u-spark-nrow%s-ncols%s-sp%s" % (n_rows, n_cols, frac)
    out_s=hdfs_root + "/s-spark-nrow%s-ncols%s-sp%s" % (n_rows, n_cols, frac)
    out_v=hdfs_root + "/v-spark-nrow%s-ncols%s-sp%s" % (n_rows, n_cols, frac)
    out_lan=hdfs_root + "/lanczos-out-nrow%s-ncols%s-sp%s" % (n_rows, n_cols, frac)
    out_stoch=hdfs_root + "/stoch-out-nrow%s-ncols%s-sp%s" % (n_rows, n_cols, frac)

    generate_matrix(project_home, gen_mat_path, n_rows, n_cols, frac, block_size, master, spark_home)
    csv_writer.writerow([spark_factorize_and_time(project_home, gen_mat_path, out_u, out_s, out_v, master, spark_home)]
                        + ['spark', n_rows, n_cols, frac, block_size])
    csv_writer.writerow([lanczos_factorize_and_time(project_home, gen_mat_path, out_lan, n_rows, n_cols, lan_rank)]
                        + ['lanczos', n_rows, n_cols, frac])
    csv_writer.writerow([stochastic_factorize_and_time(project_home, gen_mat_path, out_stoch, stoch_rank)]
                        + ['stoch', n_rows, n_cols, frac])

def main():
    rows = [20000, 40000, 100000]
    n_cols=10000
    frac=[0.5, 0.1, 0.01, 0.001]
    block_size=2
    master="yarn-client"
    spark_home="/home/juliet/bin/spark-1.3.0-bin-hadoop2.4/bin"
    lan_rank=60
    stoch_rank=20
    project_root="/home/juliet/src/svd-benchmark"
    hdfs_root="hdfs:///user/juliet/matrix"

    # Setup our env
    sub.call(["chmod +x %s/scripts/gen-matrix.sh" % project_root], shell=True)
    sub.call(["chmod +x %s/scripts/spark-svd.sh" % project_root], shell=True)
    sub.call(["chmod +x %s/scripts/lanczos-svd.sh" % project_root], shell=True)
    sub.call(["chmod +x %s/scripts/stochastic-svd.sh" % project_root], shell=True)

    with open('results/experiment.csv', 'wb') as exp_rez:
        observation_writer = csv.writer(exp_rez, delimiter=",")
        for n_rows in rows:
            for sparse_frac in frac:
                process_one_param_set(n_rows, n_cols, sparse_frac, lan_rank, stoch_rank, block_size, master,
                              spark_home, project_root, hdfs_root, observation_writer)

if __name__ == "__main__":
   main()
