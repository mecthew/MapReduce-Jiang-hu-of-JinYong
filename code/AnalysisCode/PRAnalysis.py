import copy

def loadDataSet(FilePath):
    fd = open(FilePath, "r", encoding='utf-8')
    dataSet = []
    prMap = {}
    for line in fd:
        personName = line.split("\t")[0]
        list =line.split("\t")[1]
        pr = float(list.split("#")[0])
        dataSet.append(personName)
        prMap[personName] = pr
    return dataSet, prMap

if __name__ == "__main__":
    begin = 1
    dirPath = "PR/Data"+str(begin)+"/part-r-00000"
    dataSet, prMap = loadDataSet(dirPath)
    len = dataSet.__len__()

    for i in range(len):
        maxIndex = i
        for j in range(i+1, len):
            personName = dataSet[j]
            if prMap[dataSet[maxIndex]] < prMap[personName]:
                maxIndex = j
        strName = dataSet[maxIndex]
        dataSet[maxIndex] = dataSet[i]
        dataSet[i] = strName

    prevSortArray = dataSet
    accurate = []

    for fileIndex in range(begin+1, 21):
        filePath = "PR/Data"+str(fileIndex)+"/part-r-00000"
        postDataSet, postPrMap = loadDataSet(filePath)

        for i in range(len):
            maxIndex = i
            for j in range(i+1,len):
                personName = postDataSet[j]
                if postPrMap[postDataSet[maxIndex]] < postPrMap[personName]:
                    maxIndex = j
            strName = postDataSet[maxIndex]
            postDataSet[maxIndex] = postDataSet[i]
            postDataSet[i] = strName

        count = 0
        testLen = 100
        for i in range(testLen):
            count += 1
            for bias in range(-2, 3):
                j = i + bias
                if j >= 0 and j < len and prevSortArray[i] == postDataSet[j]:
                    count -= 1
                    break
        print(str(fileIndex)+": ", count/float(testLen))
        accurate.append(1.0-count/float(testLen))
        prevSortArray = copy.deepcopy(postDataSet)

    print(accurate)
