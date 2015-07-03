## hdfs-compress 压缩hdfs文件

一般压缩hdfs指 ，数据文件的压缩 和 mapred 中间结果压缩

工程只做数据压缩，至于MR中间结果压缩减少网络IO，只需要配置 mapred-site.xml 即可，网上资料很多。

工程只实现了lzo的压缩逻辑，需要前置条件：部署lzo环境 更多参考 http://blog.yuanxiaolong.cn/blog/2015/06/28/hadoop-lzo-for-hive/

工程不够强大，目前利用 hadoop api 按目录级压缩，已测试 。还少其他压缩策略的实现及测试，或其他方式 MR 、streaming 等，欢迎提交 pull request ，thanks

---


## build

mvn clean package

---

## run

```hadoop jar hdfs-compress-0.0.1.jar <input> <output> ```

* input : 待压缩的文件或路径
* output : 输出的路径,需要一个只有1级目录的文件夹 “e.g /home/yourname/work/” ， please <output> IS /home/yourname/work/ , NOT /home/yourname/ 

属性文件

properties global.properties

