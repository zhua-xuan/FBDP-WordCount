## 作业5

191840373 朱家萱

对设计思路，实验结果等给出说明，并给出提交作业运行成功的WEB页面截图。可以进一步对性能、扩展性等方面存在的不足和可能的改进之处进行分析。

### 一、设计思路

首先单词计数的程序很容易实现，与hadoop官网的wordcount2.0采取相同的设计思路，难点在于按照单词出现的次数从大到小排列、停词的处理和筛选前100个高频词输出。wordcount中Map函数的输入键值对是行和文本，输出是单词和次数1，经过Combiner合并，Reduce函数输入是单词和频数，输出单词和累加和。由于原本的排序是MapReduce的默认排序，就需要一个函数使得全局有序，并且降序输出，就要写一个继承类指定为逆序，并在主函数里调用。Map读取分布式缓存中的停词表，延用wordcount2中的parseSkipFile，读取filename路径下的文件，将文件中的需要词加入停用词列表wordsToSkip中，一开始的每个单词在所有文件中词频和每个单词在单个文件中词频的结果都存放在临时文件中，最后将临时文件删去。

### 二、实验结果

#### 1、按照CSDN教程，在本地搭建平台

[Hadoop: Intellij结合Maven本地运行和调试MapReduce程序 (无需搭载Hadoop和HDFS环境)](https://blog.csdn.net/binbigdata/article/details/80380344?utm_medium=distribute.pc_relevant.none-task-blog-2%7Edefault%7ECTRLIST%7Edefault-2.no_search_link&depth_1-utm_source=distribute.pc_relevant.none-task-blog-2%7Edefault%7ECTRLIST%7Edefault-2.no_search_link)

安装Intellij IDEA、JDK 1.8、hadoop 3.2.1、cygwin进行配置，方便对代码进行debug，具体的配置、调错由于与实验本身关系不大，故不再赘述，只呈现idea中运行成功的截图

![image-20211030131057785](C:\Users\Fyatto_xcom\AppData\Roaming\Typora\typora-user-images\image-20211030131057785.png)

#### 2、Linux下的运行

##### （1）把要共享的文件夹挂载到虚拟机某一个文件

为了方便文件传输，由于winscp总是连接失败，故决定采用挂载本机文件夹的方式进行本机与虚拟机的文件共享。

- 出现错误：


![image-20211029085826392](C:\Users\Fyatto_xcom\AppData\Roaming\Typora\typora-user-images\image-20211029085826392.png)

- 解决方法：


在终端输入，挂载成功![image-20211029085858219](C:\Users\Fyatto_xcom\AppData\Roaming\Typora\typora-user-images\image-20211029085858219.png)

- 重启后挂载失效问题，需要在终端输入，重新挂载成功

![image-20211029214359731](C:\Users\Fyatto_xcom\AppData\Roaming\Typora\typora-user-images\image-20211029214359731.png)

##### （2）开启dfs节点

- 查看状态，正常

![image-20211029111049588](C:\Users\Fyatto_xcom\AppData\Roaming\Typora\typora-user-images\image-20211029111049588.png)

- 若不幸没有DataNode节点，只需将tmp文件里的dfs全部删除，再格式化、重启，即可重新出现DataNode

##### （3）正式运行

![image-20211029111120218](C:\Users\Fyatto_xcom\AppData\Roaming\Typora\typora-user-images\image-20211029111120218.png)

![image-20211029112417076](C:\Users\Fyatto_xcom\AppData\Roaming\Typora\typora-user-images\image-20211029112417076.png)

- 不幸报错：


![image-20211029111655670](C:\Users\Fyatto_xcom\AppData\Roaming\Typora\typora-user-images\image-20211029111655670.png)

原因是打包的jar不对，在本机重新打包jar文件并共享后，报错解决

![image-20211029114244694](C:\Users\Fyatto_xcom\AppData\Roaming\Typora\typora-user-images\image-20211029114244694.png)

- 再报错：![image-20211029114356124](C:\Users\Fyatto_xcom\AppData\Roaming\Typora\typora-user-images\image-20211029114356124.png)

  显示找不到停词文件，而传输是成功的

![image-20211029114430466](C:\Users\Fyatto_xcom\AppData\Roaming\Typora\typora-user-images\image-20211029114430466.png)

再观察命令，发现文件地址输入错了，更改命令后：

![image-20211029114936909](C:\Users\Fyatto_xcom\AppData\Roaming\Typora\typora-user-images\image-20211029114936909.png)

成功运行：

![image-20211029115042271](C:\Users\Fyatto_xcom\AppData\Roaming\Typora\typora-user-images\image-20211029115042271.png)

![image-20211029115122273](C:\Users\Fyatto_xcom\AppData\Roaming\Typora\typora-user-images\image-20211029115122273.png)

- 查看运行结果：

![image-20211029115217762](C:\Users\Fyatto_xcom\AppData\Roaming\Typora\typora-user-images\image-20211029115217762.png)

![image-20211029115724436](C:\Users\Fyatto_xcom\AppData\Roaming\Typora\typora-user-images\image-20211029115724436.png)

![image-20211030123457743](C:\Users\Fyatto_xcom\AppData\Roaming\Typora\typora-user-images\image-20211030123457743.png)

![image-20211029115745279](C:\Users\Fyatto_xcom\AppData\Roaming\Typora\typora-user-images\image-20211029115745279.png)

- 单个文集的运行结果：

![image-20211030164826483](C:\Users\Fyatto_xcom\AppData\Roaming\Typora\typora-user-images\image-20211030164826483.png)

以其中一个莎士比亚文档为例：

![image-20211030164905881](C:\Users\Fyatto_xcom\AppData\Roaming\Typora\typora-user-images\image-20211030164905881.png)

![image-20211030121727925](C:\Users\Fyatto_xcom\AppData\Roaming\Typora\typora-user-images\image-20211030121727925.png)

- Web端的显示：


![image-20211029164240223](C:\Users\Fyatto_xcom\AppData\Roaming\Typora\typora-user-images\image-20211029164240223.png)

![image-20211029163910433](C:\Users\Fyatto_xcom\AppData\Roaming\Typora\typora-user-images\image-20211029163910433.png)

![image-20211030123349637](C:\Users\Fyatto_xcom\AppData\Roaming\Typora\typora-user-images\image-20211030123349637.png)

