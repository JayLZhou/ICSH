# ICSH

This repository contains Java codes and datasets for the paper:

> Influential Community Search over Large Heterogeneous Information Networks
>
> paper: https://drive.google.com/file/d/1lUpNkra8mR5natRhtmUEhwlJf732W9-x/view?usp=share_link

## Introduction

In this paper, we study the problem of influential community search over HINs (or ICSH problem). Conceptually, a highly influential community in the HIN is a set of vertices with the same type, that are not only closely related, but also have high importance values. In particular, we introduce a novel community model for the HIN, called heterogeneous influential community (HIC), which is a set of vertices in a meta-path-based core and its induced sub-HIN has the skyline influence vector. To search HICs, we develop fast algorithms for meta-paths with two and three vertex types, respectively. Our experimental results on four real large HINs show that our solutions are effective and efficient for searching influential communities. In the future, we will study how to maintain HICs efficiently on large dynamic HINs since many real-world HINs are evolving over time.

## Environment

The codes of ICSH are implemented and tested under the following development environment:

- Hardware : Intel(R) Xeon(R) Gold 6226R 2.90GHz CPU and 256GB of memory.
- Operation System : Ubuntu 20.04.4 LTS (GNU/Linux 5.13.0-40-generic x86_64)
- Java Version : jdk 11.0.15

## Datasets

We use four real star-schema HINs: TMDB, IMDB, DBLP and DBpedia. The dataset file in this Google driven [link](https://drive.google.com/file/d/1ITK0qxARVgJGrpawvIv_66Af-CfYEEPN/view?usp=share_link). The detailed statistics and original sources of these datasets are in our paper.

## How to Run the Codes

### A. Code Compilation

The ICSH file folder is an IDEA (IntelliJ IDEA) project file and can be opened and run through IDEA as a Java project (recommended). And it is also right to use jdk tools to complilate the codes in `ICSH/src` directly.

You can complicate corresponding java main classes in the`ICSH/src/xx` :

`javac -d . *.java`

and run class files by :

`java *`

where * means java file name (as well as class name)

### B. Run Code in the IntelliJ IDEA

> You can directly run the code in the IDEA, you need to specify the meta-path and  dataset:
>
> For DBLP, you can specify the metapath: 1-1-0-0-1 (Author-Paper-Author) and queryK = 5. Then you can directly run this code in your IDEA.

```java
String metaPath_str = "1-1-0-0-1";
MetaPath metaPath = new MetaPath(metaPath_str);
DataReader dataReader = new DataReader(Config.dblpGraph, Config.dblpVertex, Config.dblpEdge, Config.dblpWeight);

Advanced2Type InfCommunities = new Advanced2Type(graph, vertexType, edgeType, weight, 5, metaPath);
Map<double[], Set<Integer>> Communities = InfCommunities.computeComm("");
```

The correspondence between the class name and the algorithm name is as follows：

+ `basic/InfComm2Type.java`: Basic2D
+ `advanced/Advanced2Type.java`: Fast2D
+ `advanced/Basic2Plus.java`: BasicHalf2D
+ `basic/InfComm3Type.java`: Basic3D
+ `advanced/Advanced3Type.java`: Fast3D
+ `advanced/BasicHType.java`: Basic4D
+ `advanced/AdvancedHType.java`: Fast4D

Before you run the code, you also need to specify the `JVM` options:

`-Djava.util.Arrays.useLegacyMergeSort=true -Xmx220000m -Xmx220000m`

**Running Example:**

You can modify the `exp/runTest.java` to specify the meta-path, dataset and queryK. Next, you can directly run this code in your IDEA

```java
// You need to specify some parameters:
// Dataset: TMDB
// queryK: e.g., 5
// Meta-Path: e.g.,Movie-Director-Movie
// Method: Fast2D
int queryK  = 5;
String metaPath_str = "0-6-4-7-0";
MetaPath metaPath = new MetaPath(metaPath_str);
String dataSetName = "tmdb";
String method = "Fast2D";
String graphDataSetPath = Config.root + "/" + dataSetName;
runTest vdTest = new runTest(graphDataSetPath, metaPath, dataSetName);
vdTest.test(queryK, method);
```

You will obtain the following results:

> The format is: [movie's popularity,  director's box office grosses]  [community members ...]

```java
[66.976776, 10.548214267]    [1818, 1697, 2085, 190, 1006, 119]
[67.66094, 6.234602552]    [0, 279, 25, 93, 2403, 3439]
[68.889395, 4.210555314]    [270, 274, 3158, 220, 157, 1725]
[73.987775, 3.170405984]    [256, 1605, 200, 9, 14, 183]
[94.815867, 2.389717088]    [898, 195, 612, 249, 1337, 172]
```

### C. Data Download

You can download the datasets from the following Google driven link:

https://drive.google.com/file/d/1ITK0qxARVgJGrpawvIv_66Af-CfYEEPN/view?usp=share_link

### D. Experimentation

The file path mentioned in the following is started with `ICSH/src/exp`
#### Reproduce all results
You can directly run the `src/exp/expTest.java` to reproduce all the experiments
```java
// 1. runEfficiency: reproduce the whole efficiency experiments
// 2. runEffectiveness: reproduce the whole effectiveness experiments (i.e., diameter, path-sim, etc.)
// 3. runMember: reproduce the whole size and number experiments
// 4. runAvgInfluence: reproduce the average influence experiments
// 5. runCaseStudy: reproduce the case-study experiment
```
for (String dataSetName : dataSetList) {
    String graphDataSetPath = Config.root + "/" + dataSetName;
    String metaPathsPath = Config.root + "/" + dataSetName;
    expTest vdTest = new expTest(graphDataSetPath, metaPathsPath, dataSetName);
    vdTest.runEfficiency(graphDataSetPath, metaPathsPath, dataSetName, kArry);

    vdTest.runEffectivenss(graphDataSetPath, metaPathsPath, dataSetName, 5); // 5 is the default queryK.
    vdTest.runMember(graphDataSetPath, metaPathsPath, dataSetName, kArry);
    if (dataSetName.equals("tmdb") || dataSetName.equals("DBLPWithWeight")) {
        vdTest.runAvgInfluence(graphDataSetPath, metaPathsPath, dataSetName, 5); // Only for TMDB (MDM and GMDMG) and DBLP (APA and TPVPT)
    }
    // CaseStudy only for the DBLP (APA)
    if (dataSetName.equals("DBLPWithWeight")) {
        vdTest.runCaseStudy(graphDataSetPath, metaPathsPath, dataSetName);
    }
}
#### Effectiveness evaluation

- effectiveness of h=2: `effectivenessComm2`
- effectiveness of h=3: `effectivenessComm3`
- compute the sizes and numbers of communities of h=2: `CommunityMemeber2.java`
- compute the sizes and numbers of communities of h=3: `CommunityMemeber3.java`
- compute the average influence value of communities : `avgInfValue.java`
- A case study :` CaseStudy.java`

#### Efficiency evaluation

- efficiency of h=2: `effencicyComm2.java`
- Scalability test  of h=3: `effencicyComm3.java`
- Scalability test  of h=4: `effencicyCommH.java`

- Scalability test  of h=2: `scalabilityTest2.java`
- Scalability test  of h=3: `scalabilityTest3.java`
- Scalability test  of h=4: `scalabilityTestH.java`
- Index construction time analysis : ScalableTest.java

#### **Detailed Reproducibility Script**

Before the reproducibility, you should download the dataset from the repo and modify file `ICSH/src/util/Config.java` based on the dataset path on your machine.

**For section 5.2 Effectiveness Evaluation**, you need to run `effectivenessComm2.java` `(effectivenessComm3.java)` to reproduce the result of compactness, similarity, and density of communities. Besides, you can run  `CommunityMemeber2.java` to compute the sizes and numbers of communities. You can run `avgInfValue.java` to reproduce the result of average influence value of communities.

**For section 5.3 Efficiency Evaluation,** you need to run `effencicyComm2.java` `(effencicyComm3.java)` get the efficiency results of our ICSH algorithms; run `scalabilityTest2.java `to get the result of scalability for two ICSH algorithms (h=2);

Note that for every file in `ICSH/src/exp`, we have some notes for each function in the file which describes the meaning of this function in brief.

### E. Contact

If you have any questions about the code or find any errors, please list them in the `issue` or contact me directly by email:

`yinglizhou@link.cuhk.edu.cn`
