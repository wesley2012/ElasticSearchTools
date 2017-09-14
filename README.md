* 清空一个type
java -jar target/ElasticSearchTools-1.0-SNAPSHOT-jar-with-dependencies.jar clear http://127.0.0.1:9200/product/goods

* 清空一个index
java -jar target/ElasticSearchTools-1.0-SNAPSHOT-jar-with-dependencies.jar clear http://127.0.0.1:9200/product

* 复制一个type
java -jar target/ElasticSearchTools-1.0-SNAPSHOT-jar-with-dependencies.jar copy http://host1/index1/type1 http://host2/index2/type2

以上功能仅在ES 1.5.2上测试过.

---

* Clear a type
java -jar target/ElasticSearchTools-1.0-SNAPSHOT-jar-with-dependencies.jar clear http://127.0.0.1:9200/product/goods

* Clear an index
java -jar target/ElasticSearchTools-1.0-SNAPSHOT-jar-with-dependencies.jar clear http://127.0.0.1:9200/product

* Copy a type
java -jar target/ElasticSearchTools-1.0-SNAPSHOT-jar-with-dependencies.jar copy http://host1/index1/type1 http://host2/index2/type2

Tested with ElasticSearch 1.5.2

