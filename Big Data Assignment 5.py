#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pyspark

# Load the adjacency list file
AdjList1 = sc.textFile("02AdjacencyList.txt")
print(AdjList1.collect())


# In[2]:


AdjList2 = AdjList1.map(lambda line : line.split())  # 1. Replace the lambda function with yours
AdjList3 = AdjList2.map(lambda x : (int(x[0]), [int(element) for element in x[1:]]))  # 2. Replace the lambda function with yours
AdjList3.persist()
print(AdjList3.collect())


# In[3]:


nNumOfNodes = AdjList3.count()
print("Total Number of nodes")
print(nNumOfNodes)


# In[4]:


print("Initialization")
PageRankValues = AdjList3.mapValues(lambda v : 1/float(nNumOfNodes))  # 3. Replace the lambda function with yours
print(PageRankValues.collect())


# In[5]:


def step4(x):
    node, (adjList, ranking) = x
    return [(dest, ranking/len(adjList)) for dest in adjList]


# In[6]:


print("Run 30 Iterations")
for i in range(1, 30):
    print("Number of Iterations")
    print(i)
    JoinRDD = AdjList3.join(PageRankValues)
    print("join results")
    print(JoinRDD.collect())
    contributions = JoinRDD.flatMap(step4)  # 4. Replace the lambda function with yours
    print("contributions")
    print(contributions.collect())
    accumulations = contributions.reduceByKey(lambda x, y : x + y)  # 5. Replace the lambda function with yours
    print("accumulations")
    print(accumulations.collect())
    PageRankValues = accumulations.mapValues(lambda v : 0.85 * v + 0.15/ float(nNumOfNodes))  # 6. Replace the lambda function with yours
    print("PageRankValues")
    print(PageRankValues.collect())


# In[7]:


print("=== Final PageRankValues ===")
print(PageRankValues.collect())


# In[8]:


PageRankValues.coalesce(1).saveAsTextFile("PageRankValues_Final_test")

