import sys,pprint
from gexf import Gexf
import codecs

input_file = codecs.open("pr-lpa2.txt", encoding="utf-8")
gexf = Gexf("2018st27","LPA")
graph=gexf.addGraph("undirected","static","LPA")

class_attr = graph.addNodeAttribute("Class", defaultValue=0, type="integer", force_id="class")
pr_attr = graph.addNodeAttribute('PageRank', defaultValue=0.0, type="double", force_id="pageranks")

i = 0
library = {}
for line in input_file:
    strs = line.split("\t")
    label_name = strs[0].split("#")
    library[label_name[1]] = i
    node = graph.addNode(i, label_name[1])
    node.addAttribute(class_attr, label_name[0])
    node.addAttribute(pr_attr, label_name[2])
    i += 1

i = 0
input_file.seek(0)

for line in input_file:
    strs = line.split("\t")
    label_name = strs[0].split("#")
    neibours = strs[1].split(";")
    src = library[label_name[1]]
    pr = 0.0
    for neibour in neibours:
        name_weight = neibour.split(":")
        dst = library[name_weight[0]]
        w = name_weight[1]
        graph.addEdge(i, str(src), str(dst), weight=w)
        i += 1

output_file=open("lpa2.gexf","wb")
gexf.write(output_file)