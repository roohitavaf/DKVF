# Orion Causal Key-Value Store

Built on top of DKVF

Published in [PaPoC 2020](https://papoc-workshop.github.io/2020/)

> Orion: Time Estimated Causally Consistent Key-Value Store
>
> *Diptanshu Kakwani, Rupesh Nasre*

Paper: https://dipkakwani.netlify.com/Orion_PaPoC.pdf

Talk: https://www.youtube.com/watch?v=xux0U1_1cmI&list=PLHHiNr_VnyWKj64PWjd9iM2OY19s9QGCY&index=6&t=0s

### Instructions

To get started, go through [DKVF wiki](https://github.com/roohitavaf/DKVF/wiki).

Design your cluster

```bash
cd tools/
java -jar ClusterDesigner.jar
```

Run cluster

```bash
java -jar ClusterManager.jar
> load_cluster config/cluster.xml
> upload_cluster
> start_cluster
```

Check cluster status

```bash
> cluster_status
```

Run experiment

```bash
> load_experiment config/exp.xml
> upload_experiment all
> start_experiment 20
```

Use properties.txt to adjust experiment parameters

```bash
fieldlength=16
operationcount=10000
recordcount=100
workload=edu.msu.cse.dkvf.ycsbWorkload.DKVFWorkload

readallfields=false

readproportion=0
updateproportion=0
insertproportion=0.5
ampinsertproportion=0
rotxproportion=0.5

threadcount=10

ampfactor=1
requestdistribution=zipfian
core_workload_insertion_retry_limit=2
```
For sample config files visit https://github.com/dipkakwani/Orion
