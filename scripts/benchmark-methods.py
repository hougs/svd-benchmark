#!/usr/bin/python
"""

"""

import subprocess as sub
import getopt
import sys

def timeProgram( program ):
    print program
    command="time -f --output=time_output.tmp %s" % program
    p = sub.call(command, shell=True)

    return execTime

def generate_matrix(wd, out_path, n_rows, n_cols, frac, block_size, master, spark_home):
    gen_mat_args = (wd.strip(), out_path, n_rows, n_cols, frac, block_size, master, spark_home)
    gen_mat_cmd = "%s/scripts/gen-matrix.sh %s %s %s %s %s %s %s" % gen_mat_args
    try:
        sub.call(gen_mat_cmd, shell=True)
    except OSError:
        print "Oops! OS Error. Could not run the command:\n %s" % gen_mat_cmd

def spark_factorize(wd, in_path, out_u, out_s, out_v, master, sparkHome):
    svd_args = (wd.strip(), in_path, out_u, out_s, out_v, master, sparkHome)
    svd_cmd = "%s/scripts/spark-svd.sh %s %s %s %s %s %s" % svd_args
    try:
        timeProgram(svd_cmd)
    except OSError:
        print "Oops! OS Error. Could not run the command:\n %s" % svd_cmd


#def process_one_param_set():


def main(argv):
    generated_matrix_path="path"
    n_rows=3
    n_cols=3
    frac=0.1
    block_size=2
    master="local"
    spark_home="/home/juliet/bin/spark-1.3.0-bin-hadoop2.4/bin"
    u_path="path"
    s_path="path"
    v_path="path"

    wd = sub.check_output("pwd", shell=True)
    gen_matScript_path = "chmod +x %s/scripts/gen-matrix.sh" % wd
    sub.call([gen_matScript_path], shell=True)
    sub.call(["chmod +x %s/scripts/spark-svd.sh" % wd], shell=True)

    # bottou spark home /home/juliet/bin/spark-1.3.0-bin-hadoop2.4/bin
    #generate_matrix(wd, generated_matrix_path, n_rows, n_cols, frac, block_size, master, spark_home)
    # factor w Spark MLLib and time it
    spark_args = (generated_matrix_path, u_path, s_path, v_path, master, spark_home)
    spark_factorize(wd, generated_matrix_path, u_path, s_path, v_path, master, spark_home)
    # factor with Mahout and time it

if __name__ == "__main__":
   main(sys.argv[1:])