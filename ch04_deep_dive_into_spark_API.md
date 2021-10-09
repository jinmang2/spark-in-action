# ch04_deep_dive_into_spark_API



## 4.1 Pair RDD 다루기

* Key-Value pair로 이루어진 자료구조를 파이썬에선 딕셔너리라 하고 SPARK에서 이와 대응대는 것이(즉, 키-값 쌍으로 구성된 RDD) **Pair RDD** 라고 한다. 
* 실무에서는 데이터를 편리하게 집계, 정렬, 조인 하기 위해서 **Pair RDD**를 종종 사용한다.



### 4.1.1 Pair RDD 생성

* 스파크는 다양한 방법으로 Pair RDD를 생성할 수 있다. 
  * `SparkContext` 의 일부 메서드는 `Pair RDD`를 기본으로 반환한다.
  * `RDD` 의 `keyBy` 변환 연산자는 `RDD` 요소로 키를 생성하는 `f` 함수를 받고 각요소를 `(f(요소), 요소)` 쌍의 튜플로 매핑한다.
* 어떤 방법이든 2-요소 튜플로 RDD 를 구성하면 `Pair RDD` 함수가 `RDD` 에 자동으로 추가된다.



### 4.1.2 기본 Pair RDD 함수

```markdown
특정 규칙에 따라 사은품을 추가하는 프로그램을 개발해 달라고 요청했다. 아래는 사은품을 추가하는 규칙이다.

1. 구매 횟수가 가장 많은 고객에게는 곰 인형을 보낸다.
2. 바비 쇼핑몰 놀이 세트를 두 개 이상 구매하면 청구 금액을 5% 할인해 준다.
3. 사전을 다섯 권 이상 구매한 고객에게는 칫솔을 보낸다.
4. 가장 많은 금액을 지출한 고객에게는 커플 잠옷 세트를 보낸다.

또한, 
* 사은품은 구매금액이 0.00 달러인 추가 거래로 기입해야 한다. 
* 마케팅 부서는 사은품을 받는 고객이 어떤 상품을 구매했는지 알려달라고 요청했다.

```

> 앞으로 진행할 파일들은 `https://github.com/spark-in-action/first-edition` 에서 다운받을 수 있다.
> 참고로 아래의 예시는 ch04 data_transactions.txt이다. 각 줄은 **구매날짜, 시간, 고객 ID, 구매 수량, 구매 금액** 이 순서대로 기입되어 있다.
>
> ```
> 2015-03-30#6:55 AM#51#68#1#9506.21
> 2015-03-30#7:39 PM#99#86#5#4107.59
> ```

* 이제 이를 **Pair ID**로 만들어 보자. 참고로 **key는 고객 ID** 이며, **값에는 각 구매 로그의 모든 정보**가 저장된다.

  * ```scala
    val tranFile = sc.textFile("first-edition/ch04/"+"ch04_data_transactions.txt") # 데이터를 로드한다.
    val tranData = tranFile.map(_.split("#")) # 데이터가 한줄로 들어올 텐데 이를 파싱한다고 생각하면 된다.
    var transByCust = tranData.map(tran=> (tran(2).toInt, tran)) # key 가 고객 ID고 값은 고객 정보인 Pair RDD가 생성되었다.
    ```

#### 4.1.2.1 키 및 값 가져오기

* `Pair RDD` 의 키 또는 값으로 구성된 새로운 RDD를 가져오려면 `keys` 또는 `values` 변환 연산자를 사용한다.

#### 4.1.2.2 키별 개수 세기

* 우리가 받은 요구사항 중 하나는 `구매 횟수가 가장 많은 고객에게 곰 인형을 보낸다.`이다. 데이터 파일을 보면 한줄은 구매 한건을 의미하므로 **줄 갯수**를 센다면 그것이 **구매횟수**이다.

* 이 작업은 `RDD 의 ` `countByKey` 행동 연산자를 사용할 수 있다. `countByKey` 는 각 키의 출현 횟수를 스칼라 `Map` 형태로 변환한다.

  * ```scala
    transByCust.countByKey()
    
    ---output---
    Map(67->7m 88-> 5, ...)
    transByCust.countByKey().values.sum
    ```

  * 참고로 `values` 와 `sum` 은 스파크 API 의 메서드가 아닌 scala의 표준 메서드이다. 

* 또 다른 스칼라 표준 메서드를 사용하므로써 구매 횟수가 가장 많았던 고객을 찾을 수 있다.

  * ```scala
    val (cid, purch) = transByCust.countByKey().toSeq,sortBy(_._2).last
    
    ---output---
    cid: Int = 53
    purch: Long = 19
    ```

  * 결과로 53번 고객이 총 19번 구매한것을 알 수 있다. 따라서 곰인형(상품 ID 4번 )을 선물로 주기 위하여 아래와 같이 코드를 추가하자. 

  * ```scala
    var comlTrans = Array(Array("2015-03-30", "11:59 PM", "53", "4", "1", "0.00"))
    ```

#### 4.1.2.3 단일 키로 값 찾기

* 우리가 받은 요구사항 중 하나는 `사은품을 받을 고객이 정확하게 어떤 상품을 구매했는지도 알려 달라`는 것이었다. 다음과 같은 `lookup()` 행동 연산자를 사용해 ID 53번 고객의 모든 구매 기록을 가져올 수 있다.

  * ```scala
    transByCust.lookup(53)
    
    ---output---
    Seq[Array[String]] = WrappedArray(Array(2015-03-30, 6:18 AM, 53, 42, 5, 2197.85))...
    ```

  * `lookup` 연산자는 결과 값을 드라이버로 전송하기 때문에 메모리에 적재할 수 있는지를 먼저 확인해야 한다.

  * 다음과 같은 scala 함수로 결과를 보기 좋게 출력해 마케팅 기획자에게 보낼 이메일에 붙여 넣을 수도 있다.

    * ```scala
      transByCust.lookup(53).foreach(tran => println(tran.mkString(", ")))
      
      ---output---
      2015-03-30, 6:18 AM, 53, 42, 5, 2197.95
      ...
      ```

#### 4.1.2.4 mapValues 변환 연산자로 Pair RDD

* 이제 우리가 받은 두번째 업무를 처리해보자 `바비 쇼핑몰 놀이 세트를 두 개 이상 구매하면 청구 금액을 5% 할인해 준다.`

  `Pair RDD` 의 `mapValues` 변환 연산자를 활용하면 키를 변경하지 않고 `Pair RDD` 에 포함된 값만 변경할 수 있다. 

  > 바비 쇼핑몰 놀이 세트의 상품 ID 는 25번이다. 

  ```scala
  transByCust = transByCust.mapValues(tran => {
    if(tran(3).toInt == 25 && tran(4).toDouble > 1) //tran: 날짜,시간,고객ID,상품ID,갯수,가격 
    		tran(5) = (tran(5).toDouble * 0.95).toString
    tran})
  ```

  * `mapValues` 가 반환하는 새로운 RDD를 동일 변수에 다시 할당해서 프로그램을 간결하게 유지했다.

#### 4.1.2.5 flatMapValues 변환 연산자로 키에 값 추가

* 그 다음으로 `사전을 다섯 권 이상 구매한 고객에게는 칫솔을 보낸다.` 가 남았다. 

  > 사전의 상품번호는 81 번, 칫솔의 상품번호는 70번이다.

* `flatMapValues`변환 연산자는 각 키 값을 **0개 또는 한개 이상** 값으로 매핑해 RDD에 포함된 요소 개수를 변경할 수 있다. 

  * 즉, 키에 새로운 값을 추가하거나 키 값을 모두 제거 할 수 있다. 
  * `flatMapValues` 를 호출하려면 `V` => `TraversableOnce[U]` 형식의 변환 함수를 전달해야한다. 

* `flatMapValues` 는 변환 함수가 반환한 컬렉션 값들을 원래 키와 합쳐 새로운 키-값 쌍을 결과 생성한다.

  * 변환 함수가 인수로 받은 값의 결과로 **빈 컬렉션** 을 반환하면 **Pair RDD**에서 제외한다.
  * 반대로 컬렉션에 두 개 이상의 값을 넣어 반환하면 결과 **Pair RDD** 에 이 값을 추가한다.

  

  ```scala
  transByCust = transByCust.flatMapValues(tran => {
    if(tran(3).toInt == 81 && tran(4).toDouble >= 5){
      val cloned = tran.clone() // 구매 기록 배열을 복제
      //복제한 배열에서 가격은 0.00 달러로 수정하고, 제품 ID 는 70으로 수정하며, 수량은 1로 감소한다.
      cloned(5) = 0.00; cloned(3) ="70"; cloned(4) = "1";
      List(tran, cloned) // 원래 요소와 추가한 요소를 반환한다.
    }
    else
    	List(tran) //원래 요소만 반환한다.
  })
  ```

  > `transByCustRDD` 는 요소를 1006개 포함한다(예제 파일에서 사전을 다섯권 이상 구매한 기록이 모두 6개이기 때문이다.)

#### 4.1.2.6 reduceByKey 변환 연산자로 키의 모든 값 병합

* `reduceByKey` 는 각 키의 모든 값을 동일한 타입의 단일 값으로 병합
* 두 값을 하나로 병합하는 merge 함수를 전달해야 하며, `reduceByKey` 는 각 키별로 값 하나만 남을 때까지 `merge` 함수를 계속 호출.



#### 4.1.2.7 reduceByKey 대신 foldByKey 사용

* `foldByKey` 는 `reduceByKey` 와 기능이 같지만, merge 함수의 인자 목록 바로 앞에 `zeroValue` 인자를 담은 또 다른 인자 목록을 추가로 전달해야 한다.

  ```scala
  foldByKey(zeroValue: V)(func: (V,V) => V): RDD[(K, V)]
  ```

* `zeroValue` 는 반드시 항등원이어야 한다. `zeroValue`는 가장 먼저 `func` 함수로 전달해 키의 첫번째 값과 병합하며, 이결과를 다시 키의 두번째 값과 병합한다. 

* 이제 우리의 마지막 계획인 `가장 많은 금액을 지출한 고객에게는 커플 잠옷 세트를 보낸다.` 를 해결해보자

  ```scala
  val amounts = transByCust.mapValues(t => t(5).toDouble) // 가격을 double 형태로 바꾼다.(원래 String 이었음)
  val totals = amouts.foldByKey(0)((p1,p2)=>p1+p2).collect() // 덧셈을 진행하니 zeroValue를 0으로 설정해주었다. 또한 foldByKey를 쓰니 계속 병합, 여기서는 '+' 니 하나만 남을때까지 계속 더한다.
  
  totals.toSeq.sortBy(_._2).last //_2 인 이유는 _1이 고객의 id 이고 _2가 총합이기 때문이다. 또한 내림차순으로 정렬했기 때문에 last를 사용한다.
  
  ---output---
  
  res1: (Int, Double) = (76,100049.0)
  ```

  > 참고로 foldByKey대신 reduceByKey를 사용해도 똑같은 결과를 얻을 수 있다.

#### 4.1.2.8 aggregateByKey 로 키의 모든 값 그루핑

* `aggregateByKey` 변환 연산자는 `zeroValue`를 받아 RDD 값을 병합한다는 점에서 `foldByKey` 나 `reduceByKey` 와 유사하지만 **값 타입을 바꿀 수 있다** 라는 점이 다르다.

* 즉, `zeroValue` 와 첫번째는 임의의 V 타입을 가진 값을 또 다른 U 타입으로 변환[(U,V)=>U] 하는 **변환 함수**이자 두번째는 첫번째 함수가 변환한 값을 두 개씩 하나로 병합하는 [(U, U) => U]  하는 **병합함수**이다.

* 예를 들어 **각 고객이 구매한 제품의 전체 목록을 가려오려면** 아래와 같다.

  * ```scala
    val prods = transByCust.aggregateByKey(List[String])())( // zeroValue에 빈 리스트를 지정한다.
  	(prods, tran) => prods ::: List(tran(3)), // 제품을 리스트에 추가한다.
    (prods1, prods2) => prods1 ::: prods2) // 키가 같은 두 리스트를 이어 붙인다.
    prods.collect()
    
    ---output---
    
    res0: Array[(String, List[String])] = Array(88, List(47.149, 147.123, ....))
  > 스칼라에서는 ::: 연산자로 두 리스트를 이어 붙일 수 있다. 




## 4.2 데이터 파이셔닝을 이해하고 데이터 셔플링 최소화

* **스파크 클러스터**
  * 병렬 연산이 가능하고 네트워크로 연결된 노드의 집합

* **데이터 파티셔닝** 

  * 데이터를 여러 클러스터 노드로 분할하는 메커니즘

* **파티션**

  * RDD 데이터의 일부(조각 또는 슬라이스)

    <img width="617" alt="image" src="https://user-images.githubusercontent.com/55227984/136666887-2d633bfa-5081-4252-9d78-883fc1ac26ca.png">

  * RDD에 변환 연산을 실행할 태스크 개수와 직결되기 때문에 파티션 개수는 매우 중요
    * 클러스터의 코어 개수보다 **서너 배** 더 많은 파티션을 사용하는것이 좋다.



### 4.2.1 스파크의 데이터 Partitioner

* RDD의 데이터 파티셔닝은 RDD 의 각 요소에 파티션 번호를 할당하는 Partitioner 객체가 수행
* Partitioner 의 구현체로 4개가 있다.
  * HashPartitioner
  * RangePartitioner



#### 4.2.1.1 HashPartitioner

* 각 요소의 자바 해시 코드를 단순한 mod 공식(`partitionIndex = hashCode % numberOfPartitions`)에 대입해 파티션 번호를 계산한다.
  * 각 요소의 파티션 번호를 거의 무작위로 결정하기 때문에 모든 파티션을 정확하게 같은 크기로 나눌 확률이 매우 적다.
  * 대규모 데이터셋을 상대적으로 적은 파티션으로 나누면 대체로 데이터 고르게 분산시킬 수 있다.

#### 4.2.1.2 RangePartitioner

* **정렬된 RDD** 의 데이터를 거의 같은 범위 간격으로 분할 가능하다.
* 하지만 직접 사용할 일은 거의 없다고 한다.



#### 4.2.1.3 Pair RDD의 사용자 정의 Partitioner

* 파티션의 데이터를 특정 기준에 따라 정확하게 배치해야 할 경우 **사용자 정의 Partitioner** 로 Pair RDD를 분할할 수 있다.
  * 각 태스크가 특정 키-값 데이터만 처리해야 할 때
* 사용자 정의 Partitioner 는 Pair RDD 에만 쓸 수 있다.
* 대부분의 Pair RDD 변환 연산자는 두가지 추가 버전을 제공한다.
  * Int 인수(변경할 파티션 개수)를 추가로 받음
    * `rdd.foldByKey(afunction, 100)`
  * 사용할 Partitioner 를 추가 인수로 받는다.(사용자 정의 Partitioner를 지정하려면 이 방법으로 해야함)
    * `rdd.foldByKey(afunction, new HashPartitioner(100))`
* `mapValues` 와 `flatMapValues`를 제외한 Pair RDD 의 변환 연산자는 모두 이 두가지 버전의 메서드를 추가로 제공한다.
* Pair RDD 변환 연산자를 호출 할 때 Partitioner 를 따로 지정하지 않으면 스파크는 부모 RDD (현재 RDD를 만드는데 사용한 RDD들) 에 지정된 파티션 개수 중 가장 큰 수를 사용.
  * 이도 없다면 `spark.default.parallelism` 매개변수에 지정된 파티션 개수로 `HashPartitioner`를 사용한다.



### 4.2.2 불필요한 셔플링 줄이기

* **셔플링**

  * 파티션 간의 물리적인 데이터 이동
  * 대표적으로 4.1.2 절 했던 예시로 설명

  <img width="625" alt="image" src="https://user-images.githubusercontent.com/55227984/136668467-96e911c3-2d6c-4960-97db-e46b7cd63910.png">

  * 위의 예시는 `transByCust`RDD를 세 파티션으로 구성해 워커 노드 세대에 분산 저장했다고 가정하고 각 파티션 별로 데이터 요소를 네개씩만 그린것 
  * `맵`태스크
    * 셔플링 바로전에 수행한 태스크
    * 중간 파일에 기록
  * `리듀스` 태스크
    * 맵 태스크  다음에 수행한 태스크
    * 맵 태스크를 읽어 들임
  * 이렇게 하는 이유는 당연이 중간 과정인 맵 태스크도 부담이 되지만 이후 리듀스 태스크는 네트워크로 전송해야 하기 때문에 **스파크 잡의 셔플링 횟수를 최소한으로 줄이도록 노력** 해야한다.
  * RDD 변환 연산에는 대부분 셔플링이 필요하지 않지만, 일부 연산은 특정 조건하에서 셔플링이 발생하므로 셔플링 횟수를 줄이기 위해 해당 조건을 잘 이해해야 한다.

#### 4.2.2.1 셔플링 발생 조건: Partitioner를 명시적으로 변경하는 경우

1. 사용자 정의 `Partitioner` 를 쓰면 반드시 셔플링 발생함
2. `HashPartitioner` 와 다른 `HashPartitioner` 를 사용해도 셔플링이 발생한다.
   * 단, 객체가 다르더라도 **동일한 파티션 개수를 지정** 했다면 같다고 간주한다.

**따라서 가급적이면 기본 Partitioner를 사용해 의도하지 않은 셔플링은 최대한 피하는것이 성능 면에서 가장 안전한 방법이다**

```scala
rdd.aggregateByKey(zeroValue, 100)(seqFunc, comboFunc).collect() // 명시적으로 파티션 갯수 설정
rdd.aggregateByKey(zeroValue, new CustomPartitioner())(seqFunc, comboFunc).collect() //사용자 정의 partitioner 적용
```



#### 4.2.2.2 셔플링 발생 조건: Partitioner를 제거하는 경우

* 대표적으로 `map` 과 `flatMap` 은 RDD 의 Partitioner 를 제거한다. 

  * 이 연산 자체로 셔플링을 발생하지 않지만, 연산자 결과 RDD에 다른 변환 연산자를 사용하면 셔플링이 발생한다.

  ```scala
  import org.apache.spark.rdd.RDD
  
  val rdd:RDD[Int] = sc.parallelize(1 to 100000)
  rdd map(x=> (x, x*x)).map(_.swap).count()
  rdd.map(x=> (x, x*x)).reduceByKey((v1, v2)=>v1+v2).count // 셔플링 발생
  ```

* `map` 이나 `flatMap` 변환 연산자 뒤에 사용하면 셔플링이 발생하는 변환 연산자

  * RDD의 Partitioner를 변경하는 Pair RDD 변환 연산자
  * 일부 RDD 변화 연산자 
    * substract, intersection, groupWith
    * sortByKey 변환 연산자
    * partitionBy 또는 shuffle=true로 설정한 coalesce 연산자

#### 4.2.2.3 외부 셔플링 서비스로 셔플링 최적화

* 셔플링을 수행하면 실행자는 다른 실행자 파일을 읽어 들여야 하지만 셔플링 도중 일부 실행자에 장애가 나면 해당 실행자가 처리한 데이터를 더이상 가져올 수 없어 데이터 흐름이 중단된다.
* **외부 셔플링 서비스**는 실행자가 중간 셔플 파일을 읽을 수 있는 단일지점을 제공하여 셔플링 데이터 교환 과정을 최적화 할 수 있다.
* 외부 셔플링 서비스를 활성화 하면 스파크는 각 워커 노드별로 외부 셔플링 서버를 시작한다.
  * `spark.shuffle.service.enabled = true` 를 하면된다.

#### 4.2.2.4 셔플링 관련 매개변수

* 스파크는 **정렬 기반 셔플링** 과 **해시 기반 셔플링**을 지원한다.
* **정렬 기반 셔플링**
  * 파일을 더 적게 생성하고 메모리를 효율적으로 사용
  * Spark 1.2 부터 기본 셔플링 알고리즘
  * `spark.shuffle.manager = sort` 로 설정.
  * **해쉬기반 셔플링**은 `spark.shuffle.manager = hash` 로 설정
* `spark.shuffle.consolidateFiles` 매개변수는 셔플링 중에 생성된 중간 파일의 통합 여부 결정
  * `ext4` 나 `XFS` 파일 시스템을 사용한다면 `true` 로 설정하자
* 집계 연산, `co-grouping` 연산으로 발생하는 셔플링 작업은 많은 메모리 리소스 필요
  * `spark.shuffle.spill` 매개변수를 사용하면 셔플링에 사용할 메모리 제한 여부 지정 가능 
    * default : true
* 메모리를 제한하면 스파크는 제한 임계치를 초과한 데이터를 디스크로 내보낸다.
  * 메모리 임계치는 `spark.shuffle.memoryFraction` 매개변수로 지정한다.
    * default : 0.2
* `spark.shuffle.spill.compress` 매개변수를 사용해 디스크로 내보낼 데이터의 압축 여부 지정 가능
  * default : true
* **결론** : 대부분 스파크의 기본설정을 그대로 사용하는 편이 좋다.





### 4.2.3 RDD 파티션 변경

* 왜 데이터 파티셔닝을 변경해야 할까?
  * 작업 부하를 효율적으로 분산시키기 위해
  * 메모리 문제를 방지하기 위해 
  * 예시
    * 일부 스파크 연산자에는 파티션 개수의 기본 값이 너무 작게 설정되어 있어 해당 값을 그대로 사용하면 파티션에 매우 많은 요소를 할당하고 메모리를 과다하게 점유해 결과적으로 병렬 처리 성능이 저하 될 수 있음
  * 따라서 `partitionBy`, `coalesece`, `repartition`, `repartitionAndSortWithinPartition` 등의 변환 연산자를 통해 RDD의 파티션을 변경해야함

#### 4.2.3.1 partitionBy

* Pair RDD에서만 사용 가능
* 인자로서 파티셔닝에 사용할 `Partitioner` 객체만 전달할 수 있다.
  * 만약 인자로 받은 `Partitioner` 객체가 기존과 동일하면 파티셔닝을 그대로 보존하고, RDD도 그대로 유지된다.
  * 반면 인자로 받은 `Partitioner` 객체가 기존과 다르면 셔플링 작업을 스케쥴링하고 새로운 RDD도 생성한다.

#### 4.2.3.2 coalesce와 repartition

* `coalesce` 연산자는 파티션 개수를 늘리는데 사용

  ```scala
  coalesce(numPartitions: Int, shuffle: Boolean = false)
  ```

  * 두번째 인자는 셔플링의 수행여부를 지정한다. 
    * 파티션 개수를 늘리기 위해선 이 값이 `true` 로 설정되어야 한다.
    * 파티션 개수를 줄이기 위해선 이 값이 `false` 로 설정되어야 한다.

* `repartition` 변환 연산자는 단순히 `shuffle` 을 `true` 를 설정해 `coalesce` 를 호출한 결과를 반환한다.



#### 4.2.3.3 repartionAndSortWithinPartition

* RDD의 파티션을 변경할 수도 있다.
* 정렬 가능한 RDD(즉, 정렬 가능한 키로 구성된 Pair RDD)에서만 사용할 수 있다.
* 새로운 Partitioner 객체를 받아 각 파티션 내에서 요소를 정렬한다.
* 셔플링 단계에서 정렬 작업을 함께 수행하기 때문에 repartition을 호출한 후 직접 정렬하는것보다 성능이 더 좋다.



### 4.2.4 파티션 단위로 데이터 매핑

* RDD의 전체 데이터뿐 아니라 RDD의 각 파티션에 개별적으로 매핑 함수를 적용할 수 있다.
* 각 파티션 내에서만 데이터가 매핑되도록 기존 변환 연산자를 최적화해 셔플링을 억제 할 수 있다.

#### 4.2.4.1 mapPartitions 와 mapPartitionsWithIndex

* `mapPartitions` 는 매핑 함수를 인수를 받는다는 점에서 `map` 과 동일하지만 매핑함수를 `Iterator[T] => Iterator[U]` 의 시그니처로 정리해야 한다는 점은 다르다. 
* `mapParitions` 의 매핑 함수는 각 파티션의 모든 요소를 반복문으로 처리하고 새로운 RDD 파티션을 생성한다.
* `mapPartitionsWithIndex` 는 매핑 함수에 파티션 번호가 함께 전달된다는 점이 다르다.
* 파티션 단위로 매핑하면 파티션을 명시적으로 고려하지 않는 다른 일반 변환 연산자보다 효율적으로 해결할 수 있다. 예를 들어 매핑함수에 연산이 많이 필요한 작업을 구현할 때는 각 요소별로 매핑함수를 호출하는 것보다 파티션당 한번 호출하는게 좋다.



#### 4.2.4.2 파티션의 데이터를 수집하는 glom 변환 연산자

* 각 파티션의 모든 요소를 배열 하나로 모아 이 배열들을 요소로 포함하는 새로운 RDD를 반환한다.

* 새로운 RDD에 포함된 요소개수는 이 RDD의 파티션 개수와 동일하다.

  ```scala
  val list = List.fill(500)(scala.util.Random.nextInt(100))
  val rdd = sc.parallelize(list, 30).glom()
  
  rdd.collect()
  rdd.count()
  
  ---output---
  res1: Long = 30
  ```

  

## 4.3 데이터 조인, 정렬, 그루핑

* 아래와 같은 요구사항을 요청받았다.
  1. 어제 판매한 상품 이름과 각 상푸별 매출액 합계(알파벳 오름차순으로 정렬할 것)
  2. 어제 판매하지 않은 상품 목록
  3. **전일 판매 실적 통계** : 각 고객이 구입한 상품의 평균가격, 최저 가격 및 최고 가격, 구매 금액 합계
* 스파크 코어 API를 사용해 이를 해결해 보자



### 4.3.1 데이터 조인







## 4.6 요약

* `Pair RDD` 는 **키와 값** 튜플로 구성된 RDD다.
* 스칼라의 `Pair RDD` 는 Pair RDD에 특화된 함수들이 구현된 `PairRDDFunctions` 클래스의 객체로 **암시적 변환**된다.
* `CountByKey`는 **각 키의 출현 횟수**를 담은 객체를 반환한다.
* `flatMapValues` 는 **각 키 값을 0개 또는 한개 이상의 값으로 매핑해 RDD에 포함된 요소 개수를 변경**할 수 있다.
* `reduceByKey` 와 `foldByKey` 는 각 키의 모든 값을 동일한 타입의 **단일 값** 으로 병합한다.
* `aggregateByKey` 도 **값을 병합**하는데 사용하지만, `reduceByKey` 나 `foldByKey` 와 달리 **값 타입을 변경**할 수 있다.
* 스파크의 **데이터 파티셔닝**은 **데이터를 여러 클러스터 노드로 분할**하는 메커니즘이다.
* 파티션 개수가 데이터를 클러스터에 분배하는 과정에 영향을 미치고 해당 RDD 변환 연산을 실행할 태스크 갯수와 직결 되기 때문에  RDD의 **파티션 개수** 는 매우 중요하다.
* RDD의 데이터 파티셔닝은 RDD의 각 요소에 파티션 번호를 할당하는 **Partitioner** 객체로 수행한다. Spark는 구현체로 **HashPartitioner** 와 **RangePartitioner** 를 제공한다.
* **셔플링**은 **파티션 간 물리적인 데이터 이동**을 의미한다. 셔플링은 **새로운 RDD 파티션을 만들려고 여러 파티션의 데이터를 합쳐야 할 때 발생** 한다.
* **셔플링 횟수를 최소화하는 것** 이 성능향상의 지름길이다.
* 파티션 단위로 동작하는 RDD 연산에는 **mapPartitions**와 **mapParitionsWithIndex**가 있다. 

## 참고 

* Scala val과 var의 차이
  * `val` 은 변경이 불가능 하지만 `var` 은 변경할 수 있다.
   <img width="263" alt="image" src="https://user-images.githubusercontent.com/55227984/136650722-2612e8fb-df79-4376-b1b5-971a809e70c5.png">



