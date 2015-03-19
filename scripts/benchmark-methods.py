#!/usr/bin/python
"""

"""

import subprocess as sub
import csv
import sys

def time_it(program):
    command="/usr/bin/env time -f \"%E\" " +  program
    try:
        p = sub.check_output(command, shell=True, stderr=sub.STDOUT)
        out = p.communicate()
    except sub.CalledProcessError, err:
        print "The command used to launch the failed subprocess is was [%s]." % err.cmd
        print "The output of the command was [%s]" % err.output
        raise
    return out

def generate_matrix(wd, out_path, n_rows, n_cols, frac, block_size, master, spark_home):
    gen_mat_args = (wd.strip(), out_path, n_rows, n_cols, frac, block_size, master, spark_home)
    gen_mat_cmd = "%s/scripts/gen-matrix.sh %s %s %s %s %s %s %s" % gen_mat_args
    try:
        sub.call(gen_mat_cmd, shell=True)
    except OSError:
        print "Oops! OS Error. Could not run the command:\n %s" % gen_mat_cmd

def spark_factorize_and_time(wd, in_path, out_u, out_s, out_v, master, sparkHome):
    svd_args = (wd.strip(), in_path, out_u, out_s, out_v, master, sparkHome)
    svd_cmd = "{%s/scripts/spark-svd.sh %s %s %s %s %s %s 2> spark.logs; }" % svd_args
    try:
        elapsed_time = time_it(svd_cmd)
    except OSError:
        print "Oops! OS Error. Could not run the command:\n %s" % svd_cmd
    return elapsed_time

def lanczos_factorize_and_time(wd, in_path, out_path, n_rows, n_cols, rank):
    lan_args = (wd, in_path, out_path, n_rows, n_cols, rank)
    lan_cmd = "{%s/scripts/lanczos-svd.sh %s %s %s %s %s 2> spark.logs; }" % lan_args
    try:
        elapsed_time = time_it(lan_cmd)
    except OSError:
        print "Oops! OS Error. Could not run the command:\n %s" % lan_cmd
    return elapsed_time

def stochastic_factorize_and_time(wd, in_path, out_path, rank):
    stoch_args = (wd, in_path, out_path, rank)
    stoch_cmd = "{%s/scripts/lanczos-svd.sh %s %s %s 2> spark.logs; }" % stoch_args
    try:
        elapsed_time = time_it(stoch_cmd)
    except OSError:
        print "Oops! OS Error. Could not run the command:\n %s" % stoch_cmd
    return elapsed_time


def process_one_param_set(wd, gen_mat_path, n_rows, n_cols, frac, out_u, out_s, out_v, out_lan, lan_rank, stoch_rank,
                          out_stoch, block_size, master, spark_home, csv_writer):
    generate_matrix(wd, gen_mat_path, n_rows, n_cols, frac, block_size, master, spark_home)
    csv_writer.writerow([spark_factorize_and_time(wd, gen_mat_path, out_u, out_s, out_v, master, spark_home)]
                        + ['spark', n_rows, n_cols, frac, block_size])
    csv_writer.writerow([lanczos_factorize_and_time(gen_mat_path, out_lan, n_rows, n_cols, lan_rank)]
                        + ['lanczos', n_rows, n_cols, frac])
    csv_writer.writerow([stochastic_factorize_and_time(wd, gen_mat_path, out_stoch, stoch_rank)]
                        + ['stoch', n_rows, n_cols, frac])

def main(argv):
    gen_matrix_path="path"
    n_rows=3
    n_cols=3
    frac=0.1
    block_size=2
    master="local"
    spark_home="/home/juliet/bin/spark-1.3.0-bin-hadoop2.4/bin"
    u_path="path"
    s_path="path"
    v_path="path"
    out_lan="lanpath"

    # Setup our env
    wd = sub.check_output("pwd", shell=True).strip()
    sub.call(["chmod +x %s/scripts/gen-matrix.sh" % wd], shell=True)
    sub.call(["chmod +x %s/scripts/spark-svd.sh" % wd], shell=True)
    sub.call(["chmod +x %s/scripts/lanczos-svd.sh" % wd], shell=True)
    sub.call(["chmod +x %s/scripts/stochastic-svd.sh" % wd], shell=True)


    # bottou spark home /home/juliet/bin/spark-1.3.0-bin-hadoop2.4/bin
    with open('results/experiment.csv', 'wb') as exp_rez:
        observation_writer = csv.writer(exp_rez, delimiter=",")
        for n_rows in [10000, 20000, 400000]:
            process_one_param_set(wd, gen_matrix_path, n_rows, n_cols, frac, u_path, s_path, v_path, out_lan, block_size, master,
                              spark_home, observation_writer)

if __name__ == "__main__":
   main(sys.argv[1:])