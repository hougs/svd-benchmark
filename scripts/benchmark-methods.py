#!/usr/bin/env python
"""

"""

import subprocess as sub
import csv
import time

def time_it(command):
    #command="/usr/bin/env time -f \"%E\" " +  program + " 2>&1 | tail -1"
    try:
        start_time = time.clock()
        sub.check_output(command, shell=True, stderr=sub.STDOUT)
        end_time = time.clock
    except sub.CalledProcessError, err:
        print "The command used to launch the failed subprocess is was [%s]." % err.cmd
        print "The output of the command was [%s]" % err.output
        raise
    return end_time - start_time

def generate_matrix(project_root, out_path, n_rows, n_cols, frac, n_partitions, master, spark_home):
    gen_mat_args = (project_root, out_path, n_rows, n_cols, frac, n_partitions, master, spark_home)
    gen_mat_cmd = "%s/scripts/gen-matrix.sh %s %s %s %s %s %s %s" % gen_mat_args
    print gen_mat_cmd
    try:
        sub.call(gen_mat_cmd, shell=True)
    except OSError:
        print "Oops! OS Error. Could not run the command:\n %s" % gen_mat_cmd

def spark_factorize_and_time(project_root, in_path, out_u, out_s, out_v, master, sparkHome, rank):
    svd_args = (project_root, in_path, out_u, out_s, out_v, master, rank, sparkHome)
    svd_cmd = "%s/scripts/spark-svd.sh %s %s %s %s %s %s %s" % svd_args
    try:
        elapsed_time = time_it(svd_cmd)
    except OSError:
        print "Oops! OS Error. Could not run the command:\n %s" % svd_cmd
        elapsed_time = "failed"
    return elapsed_time

def lanczos_factorize_and_time(project_root, in_path, out_path, n_rows, n_cols, rank):
    lan_args = (project_root, in_path, out_path, n_rows, n_cols, rank*2)
    lan_cmd = "%s/scripts/lanczos-svd.sh %s %s %s %s %s" % lan_args
    try:
        elapsed_time = time_it(lan_cmd)
    except OSError:
        print "Oops! OS Error. Could not run the command:\n %s" % lan_cmd
        elapsed_time = "failed"
    return elapsed_time

def stochastic_factorize_and_time(project_root, in_path, out_path, rank):
    stoch_args = (project_root, in_path, out_path, rank)
    stoch_cmd = "%s/scripts/stochastic-svd.sh %s %s %s" % stoch_args
    try:
        elapsed_time = time_it(stoch_cmd)
    except OSError:
        print "Oops! OS Error. Could not run the command:\n %s" % stoch_cmd
        elapsed_time = "failed"
    return elapsed_time

def make_hfds_path(matrix_name, n_rows, n_cols, frac, hdfs_root):
    return hdfs_root + "/%s-nrow%s-ncols%s-sp%s" % (matrix_name, n_rows, n_cols, frac)

def process_one_param_set(n_rows, n_cols, frac, rank, n_partitions, master, spark_home,
                          project_home,
                          hdfs_root, csv_writer, n_samples):
    # setup paths for these matrices
    gen_mat_path=make_hfds_path("gen-matrix", n_rows, n_cols, frac, hdfs_root)
    out_u=make_hfds_path("u-spark", n_rows, n_cols, frac, hdfs_root)
    out_s=make_hfds_path("s-spark", n_rows, n_cols, frac, hdfs_root)
    out_v=make_hfds_path("v-spark", n_rows, n_cols, frac, hdfs_root)
    out_lan=make_hfds_path("lanczos", n_rows, n_cols, frac, hdfs_root)
    out_stoch=make_hfds_path("stochastic", n_rows, n_cols, frac, hdfs_root)

    print "Generating matrix that will be stored in [%s]." % gen_mat_path
    generate_matrix(project_home, gen_mat_path, n_rows, n_cols, frac, n_partitions, master,
     spark_home)

    for idx in range(n_samples):
        print "Spark SVDing the matrix stored in [%s]. On iteration [%s]." % (gen_mat_path, idx)
        csv_writer.writerow([spark_factorize_and_time(project_home, gen_mat_path, out_u, out_s,
                                                      out_v, master, spark_home, rank)]
                        + ['spark', n_rows, n_cols, frac])
    for idx in range(n_samples):
        print "Mahout Lanczos SVDing the matrix stored in [%s]. On iteration [%s]." % (gen_mat_path, idx)
        csv_writer.writerow([lanczos_factorize_and_time(project_home, gen_mat_path, out_lan, n_rows, n_cols, 3 * rank)]
                        + ['lanczos', n_rows, n_cols, frac])

    for idx in range(n_samples):
        print "Mahout Stochastic SVDing the matrix stored in [%s]. On iteration [%s]." % (gen_mat_path, idx)
        csv_writer.writerow([stochastic_factorize_and_time(project_home, gen_mat_path, out_stoch, rank + 5)]
                        + ['stoch', n_rows, n_cols, frac])

def main():
    rows = [1000000]#, 15000000, 20000000
    # .8Gb and 80GB gramian matrices for this many columns. Spark needs at least twice this in driver memory.
    n_cols=[1000000, 2000000, 4000000]
    frac=[0.2]
    n_partitions=60
    master="yarn-client"
    spark_home="/home/juliet/bin/spark-1.3.0-bin-hadoop2.4"
    rank=10
    project_root="/home/juliet/src/svd-benchmark"
    hdfs_root="hdfs:///user/juliet/matrix"
    n_samples=3

    # Setup our env
    sub.call(["chmod +x %s/scripts/gen-matrix.sh" % project_root], shell=True)
    sub.call(["chmod +x %s/scripts/spark-svd.sh" % project_root], shell=True)
    sub.call(["chmod +x %s/scripts/lanczos-svd.sh" % project_root], shell=True)
    sub.call(["chmod +x %s/scripts/stochastic-svd.sh" % project_root], shell=True)

    with open('results/experiment.csv', 'wb') as exp_rez:
        observation_writer = csv.writer(exp_rez, delimiter=",")
        for n_rows in rows:
            for sparse_frac in frac:
                process_one_param_set(n_rows, n_cols, sparse_frac, rank, n_partitions, master,
                              spark_home, project_root, hdfs_root, observation_writer, n_samples)

if __name__ == "__main__":
   main()
