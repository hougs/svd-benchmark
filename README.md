# SVD Benchmarking
This project is an attempt at comparing the performance various 
distrubted SVD implementations, under reasonable configurations
for their underlying frameworks:

* Mahout's [Lanczos' Method](http://en.wikipedia.org/wiki/Lanczos_algorithm) based SVD.
* Mahout's [Stochastic SVD](http://arxiv.org/abs/0909.4061) based SVD.
* Spark's Lanczos' Method based SVD solver.

## Results
*Caveat Emptor*: Performance benchmarks are for charlatans and snake oil peddlers. Below are my performance benchmarks.

When I spent a reasonable amount of time configuring and tuning my cluster and jobs,
these were the timing results I got:

![vary columns of matrix](/results/varycols.png)

![vary rows of matrix](/results/varyrows.png)

![vary sparsity of matrix](/results/varysparsity.png)

If you use this repo to run a comparison, your results are very welcome in the form of a PR!


