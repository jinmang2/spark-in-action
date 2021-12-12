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
    Map(67->7, 88-> 5, ...)
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




## 4.2 데이터 파티셔닝을 이해하고 데이터 셔플링 최소화

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
* Partitioner 의 구현체로 3개가 있다.
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

  ```markdown
  1. 어제 판매한 상품 이름과 각 상품별 매출액 합계(알파벳 오름차순으로 정렬할 것)
  2. 어제 판매하지 않은 상품 목록
  3. **전일 판매 실적 통계** : 각 고객이 구입한 상품의 평균가격, 최저 가격 및 최고 가격, 구매 금액 합계
  ```
  
* 스파크 코어 API를 사용해 이를 해결해 보자

### 4.3.1 데이터 조인

* 우선 `상품별 매출액 합계` 부터 처리하자. 그러기 위해서 방금 까지 사용했던 데이터를 다음과 같이 변환해 주자

  ```scala
  val transByProd = tranData.map(tran => (tran(3).toIntt, tran)) // 상품 ID 를 키로 생성
  
  val totalsByProd = transByProd.mapValues(t => t(5).toDouble).reduceByKey{case(tot1, tot2) => to1 + to2} // 매출액 합계 계산
  
  val products = sc.textFile("first-edition/ch04/ch04_data_products.txt").map(line => line.split("#")).map(p=>(p(0).toInt, p)) //상품이름 정보를 로드한다. 
  ```



#### 4.3.1.1 RDBMS 와 유사한 조인 연산자

* 스파크의 조인 연산은 `Pair RDD` 에서만 사용가능하다. 
  * `join` : RDBMS 의 내부 조인과 동일하며, 첫번째 `PairRDD`[(K,V) ]와 두번째 `PairRDD[(K,W)]` 에서 키가 동일한 모든 값의 조합이 포함된 `PairRDD[K,(V,W)]` 가 생성된다. 어느 한쪽에만 있는 키의 요소는 결과  RDD에서 제외된다.
  * `leftOuterjoin` : `(K, (V,W))` 대신 `(K, (V, Option(W)))` 타입의 `Pair RDD` 를 반환한다. 첫번째 RDD 에만 있는 키의 요소는 결과 RDD 에서 `(key, (V, None))` 타입으로 저장되고 두번째 RDD 에만 있는 키 요소는 결과 RDD 에서 제외된다.
  * `rightOuterJoin` : `leftOuterjoin` 과 반대로 `(K, Option(V), W)` 타입의 Pair RDD 를 반환한다. 두번째에만 있는 key 요소는 `(K, (None, W))` 첫번째 RDD 에만 있는 키의 요소는 결과 RDD 에서 제외된다.
  * `fullOuterJoin` : `(K, Option(V), Option(W))` 타입의 Pair RDD 를 반환한다. 두 RDD 중 어느 한쪽만 있는 키 요소는 결과 RDD 에 `K, (v, None))` 또는 `K, (None, w)` 타입으로 저장된다.
  * 다른 `Pair RDD` 변환 연산자 처럼 `join` 연산자에 `Partitioner` 객체나 파티션 개수 를 지정할 수 있다.
    1. 파티션 개수만 지정시
      * `HashPartitioner` 를 사용한다. 
    2. `Partitioner`를 지정하지 않으면(또한, 갯수도 지정하지 않으면)
      * `join` 할 스파크 중에서 첫번째 RDD의 `Partitioner`를 사용한다. 

* 다음과 같이 `join` 연산자를 사용하여 결합을 진행하자

  ```scala
  val totalsAndProds = totalsByProd.join(products)
  totalsAndProds.first()
  
  ---output---
  res0: (Int, (Double, Array[String])) = (84, (75192.53, Array(84, Cyanocobalamin, 2044.61, 8)))
  ```
  
* 다음으로 어제 판매하지 않은 상품 목록은 어떻게 얻을 수 있을까? 이떈 left 혹은 right join 을 사용해 여집합을 이용하면 된다.

  ```scala
  val totalsWithMissingProds = products.leftOuterJoin(totalsByProd)
  val totalsWithMissingProds = totalsByProd.rightOuterJoin(products)
  ```

  * 두 객체의 결과는 동일하지만 `None` 이 삽입되는 위치가 다르다. `rightOuterJoin` 을 사용했다 가정해보면 아래와 같은 코드를 사용하면 된다.

    ```scala
    val missingProds = totalWithMissingProds.filter(x => x._2._1 == None).map(x._2._2)
    
    missingProds.foreach(p => println(p.mkString(", ")))
    
    ---output---
    43, Tomb Raider PC, 2718.14, 1
    ....
                     
    ```

#### 4.3.1.2 subtract 나 subtractByKey 변환 연산자로 공통 값 제거

* 위 방법보다 훨씬 간단한 방법이 있다. 

  * `subtract` 는 첫번째 RDD 에서 두번째 RDD 의 요소를 제거한 여집합을 반환한다.

    * `Pair RDD` 분만 아니라 일반 RDD에서도 사용가능하다.
    * 키나 값만을 비교하는 것이 아닌 요소 전체를 비교해 제거 여부를 판단

  * `subtractByKey` 는  Pair RDD 에서만 사용가능한 메소드다.

    * 첫번째 RDD 의 키-값 쌍 중 두번째 RDD 에 포함안된 키의 요소들로 RDD를 구성해 반환한다. 

      ```scala
      val missingProds = products.subtractByKey(totalsByProd).values
      
      missingProds.foreach(p=>println(p.mkString(",")))
      ---output---
      20, LEGO Elves, 4589.79, 4
      ```

      

#### 4.3.1.3 cogroup 변환 연산자로 RDD 조인

* 더 간단한 방법이 있다.

  * `cogroup` 연산자를 사용해 어제 판매한 상품과 그렇지 않은 상품 목록을 한꺼번에 찾을 수 있다.

  * 여러 RDD 값을 키로 그루핑하고 각 RDD 의 키별 값을 담은 Iterable 객체를 생성후 이 Iterable 객체 배열을 Pair RDD 값으로 반환한다.

    ```scala
    val prodTotCogroup = totalsByProd.cogroup(products)
    
    prodTotCgroup.filter(x => x._2._1.isEmpty).foreach(x=> println(x._2._2.head.mkString(", ")))
    // 두 RDD 중 하나에만 키가 등장한 경우 다른 쪽 RDD는 비어 있다. 따라서 어제 팔리지 않은 상품은 위처럼 편하게 나타낼 수 있다.
    ---output---
    43, Tomb Raider PC, 2718.14, 1
    ...
    ```

    * `x._2._1` 은 `totalsByProd` RDD 값을 담은 `Iterator` 이며 `x._2._2` 는 String 배열 형태의 상품 정보를 담은 Iterator이다. 

#### 4.3.1.4 Intersection 변환 연산자 사용

* `intersection` 은 타입이 동일한 두 RDD 에서 교집합을 새로운 RDD를 반환한다.

* 예를들어, 여러 부서의 각기 다른 상품으로 구성된 `totalByProd` 에서 특정 부서의 상품만 분석한다고 가정 할 경우 아래와 같은 코드를 사용할 수 있다.

  ```scala
  totalsByProd.map(_._1).intersection(products.map(_._1))
  ```

#### 4.3.1.5 cartesian 변환 연산자로 RDD 두개 결합

* 두 RDD 곱을 의미하는 것으로 첫번째 `RDD[T]` 와 두번째 `RDD[U]` 의 요소로 만들 수 있는 모든 조합이 (T, U) 쌍의 튜플 형태로 구성된다.

* 아래와 같은 예시를 사용하면 더 빠르게 이해할 수 있다.

  ```scala
  val rdd1 = sc.parallelize(List(7,8,9))
  val rdd2 = sc.parallelize(List(1,2,3))
  rdd1.cartesian(rdd2).collect()
  
  ---output---
  res0: Array[(Int, Int)] = Array((7,1), (7,2), (7,3), ...)
  ```

  

#### 4.3.1.6 zip 변환 연산자로 RDD 조인

* `zip` 과 `zipPartitions` 변환 연산자는 모든 RDD 에서 사용 가능하다.

* RDD[T]의 zip 연산자에 RDD[U] 를 전달하면 zip 연산자는 RDD[(T, U)] 를 반환한다.

* 단, 아래와 같은 제약이 있다.

  1. 두 RDD 의 파티션 개수가 다르면 안된다.
  2. 두 RDD의 모든 파티션이 서로 동일 한 개수의 요소를 포함해야 한다.

* 아래의 예시는 더 빠른 이해를 돕는다.

  ```scala
  val rdd1 = sc.parallelize(List(1,2,3))
  val rdd2 = sc.parallelize(List("n4", "n5"," n6"))
  rdd1.zip(rdd2).collect()
  
  ---output---
  res1: Array[(Int, String)] = Array((1, "n4"), (2, "n5"), (3, "n6"))
  ```



#### 4.3.1.7 zipPartitions 변환 연산자로 RDD 조인

* 여러 파티션을 결합하는 변환 연산자

* 모든 RDD 는 **파티션 갯수가 동일**해야 하지만, 파티션에 **포함된 요소 개수가 같을 필요는 없다**.

* `zipPartitions` 는 인자 목록을 2개 받음

  * 첫번째는  `zipPartitions` 로 결합할   RDD를 전달한다.
  * 두번째는 조인 함수를 정의해 전달한다.

* `zipPartitions` 변환 연산자의 첫번째 인자에 `preservesPartitioning` 이라는 선택 인수를 추가할 수 있다. (기본 값 false)

* 조인 함수가 데이터의 파티션을 보존한다고 판단하면 이 인수를 true로 설정할 수 있다.

* 이 인수를  `false`로 설정하면 결과 RDD 의 `Partitioner`를 제거해, 다른 변환 연산자 실행시 셔플링이 발생한다.

* 예시

  ```scala
  val rdd1 = sc.parallelize(1 to 10, 10)
  val rdd2 = sc.parallelize(1 to 8).map(x=> "n"+x), 10)
  rdd1.zipPartitions(rdd2, true)((iter1, iter2) => {
    iter1.zipAll(iter2, -1, "empty") //zipall 을 사용하면 컬렉션의 크기가 달라도 결합할 수 있다.
    																 //또한 첫번째 Iterator의 나머지 요소에 empty 라는 모조값 결합
    																 //두번째 Iterator 요소가 더 많으면 -1을 사용한다.
    .map({case(x1, x2)=>x1+"-"+x2})
  }).collect()
  
  res1: Array[String] = Array(1-empty, 2-n1, 3-n2, 4-n3, 5-n4, 6-empty, ...)
  ```
  





### 4.3.2 데이터 정렬

* 우리는 `totalsAndProds` 를 사용하여 상품이름과 상품별 매출액 목록을 RDD로 추출했다. 이제 **상품이름을 알파벳 순으로 정렬**해보자

* 정렬로 사용되는 변환 연산자는 주로 아래와 같다.

  * `repartitionAndSortWithinPartition`
  * `sortByKey`
  * `sortBy`

* 우선  `sortBy` 부터 알아보자

  ```scala
  val sortedProds = totalsAndProds.sortBy(_._2._2(1))
  sortedProds.collect()
  
  //현재 PairRDD 인 totalsAndProds의 값은 (매출액, 상품정보 배열) 쌍으로 이루어져 있다. 
  //_._2._2(1) 은 튜플 중 상품 정보 배열의 두번째요소(상품이름)을 참조한다.
  
  ---output---
  res0: Array[(Double, Array[(String)])] = Array(90,(48601.89, Array(90a, AMBROSIA TRIFIDA POLLEN, 5887.49, 1)), (94, (31049.07, Array(94,...))))
  ```
  
* 키가 복합객체인 경우 정렬작업이 힘들수 있다.

* 암시적 변환으로 키-값 튜플로 구성된 `RDD` 에서만 `Pair RDD`의 변환 연산자를 사용할 수 있는 것처럼 `sortByKey`와 `repartitionAndSortWithinPartition` 도 정렬 가능한 클래스를 `PairRDD` 의 키로 사용할 때만 호출 가능하다.

* 스칼라에서는 `Ordered` trait 또는 `Ordering` trait 을 사용하여 클래스를 정렬 가능하게 만들고 이 클래스를 키 또는 값으로 가진  RDD를 정렬 할 수 있다.

#### 4.3.2.1 Ordered trait 으로 정렬 가능한 클래스 생성

* 정렬 가능한 클래스를 만들기 위해서 할 수 있는 첫번째 방법은 **자바의 Comparable 인터페이스와 유사한 스칼라의 Ordered trait을 확장(extends)** 하는 것이다.
* `compare` 함수는 호출된 객체(`this` 객체) 가 인수로 받은 객체 보다 더 클때 **양의 정수** 더 작으면 **음수 정수** 객체가 동일하면 **0**을 반환한다.
* 참고로 스칼라가 암시적 변환으로  `Ordered` 를 `Ordering` 으로 바꾸기 때문에 `Orderd` 를 사용해도 안전하게 `sortByKey`를 호출할 수 있다. 

* 예를 들어 다음 케이스 클래스를  RDD 키로 사용해 직원들의 성을 기준으로 요소를 정렬할 수 있다. 

   ```scala
    case Class Employee (lastName: String) extends Ordered[Employee]{
      override def compare(that: Employee) = this.lastName.compare(that.lastName)
    }
   ```

    

#### 4.3.2.2 Ordering trait으로 정렬 가능한 클래스 생성

* 정렬 가능한 클래스를 만들 수 있는 두번째 방법은 **자바의  Comparator 인터페이스와 유사한 Ordering trait을 확장(extends) ** 하는 것이다.
   ```scala
   implicit val emplOrdering = new Ordering[Employee] {
     orverride def compare(a: Employee, b: employee) = a.lastName.compare(b.lastName)
   }
   
   //or
   
   implicit def emplOrider: Ordering[Employee] = Ordering.by(_.lastName)
   ```



#### 4.3.2.3 이차 정렬

* 예를들어 **고객 ID로 구매기록을 그루핑 한 후 구매 시각을 기준으로 각 고객의 구매 기록을 정렬**하는 작업이 필요하다고 가정해보자.

* 이런 작업은 **groupByKeyAndSortValues** 변환 연산자가 적합하다.

* 해당 연산자를 호출하기 위해선 `(K,V)` 쌍으로 구성된 RDD 와 `Ordering[V]` 타입의 암시적 객체를 스코프 내에 준비하고, 연산자에 `Partitioner` 객체나 파티션 개수를 전달해야 한다.

* `groupByKeyAndSortValues` 는 `(K, Iterable(V))` 타입의 RDD 를 반환하고, `Iterable(V)` 는 암시적 `Ordering` 객체에서 정의한 기준으로 정렬된 값들을 포함한다. 

* 하지만 해당 메소드는 정렬 작업에 앞서 키별로 값을 그루핑 하기에 상당한 메모리와 네트워크 리소스가 든다.

* 또는 그루핑 연산 없이도 이차 정렬을 효율적으로 수행할 수 있다.

  1. `RDD[(K,V)]` 를 `RDD[((K,V), null)]` 로 매핑한다. (ex. `rdd.map(kv => (kv, null))`)
  2. 사용자 정의 `Partitioner ` 를 이용하여 새로 매핑한 복합 키(K,V) 에서 K 부분만으로 파티션을 나누면 같은 K 가 있는 요소들을 동일한 파티션에 모을 수 있다.
  3. `repartitionAndSortWithinPartition` 변환 연산자에 사용자 정의 `Partitioner` 를 인수로 전달하여 연산자를 호출한다. 그렇게 되면 해당 연산자는 전체 복합 키(K,V) 를 기준으로 각 파티션 내 요소들을 정렬한다. 복합 키로 요소를 정렬하는 순서는 먼저 키를 정렬 기준으로 쓴 후 값을 사용한다.

  * 아래는 이를 도식화 한것이다. 해당 방법은 값을 그루핑 하지 않아 성능면에서 더 우수하다.
     ![image](https://user-images.githubusercontent.com/55227984/136739894-b438eaa5-ab1d-4070-b54f-0e83c523ca97.png)

#### 4.3.2.4 top과 takeOrdered 로 정렬된 요소 가져오기

* 해당 연산자들을 사용해서 상위, 하위 몇개의 요소를 가져올 수 있다.
* 해당 요소는 스코프 내에 정의된 implicit Ordering[T] 에 따라 객체를 기준으로 결정된다.
* `top` 과 `takeOrdered`는 키를 기준으로 요소를 정렬하는 것이 아닌 `(K, V)` 튜플을 기준으로 요소를 정렬한다. 
* `Pair RDD` 해당 메소드들을 사용하기 위해선 암시적 Ordering[( K, V)] 객체를 스코프 내에 정의해야 한다.
  * 단, 키와 값이 기본 타입일 경우 이미 Ordering 객체가 정의되어 있다.
* `top` 과 `takeOrdered` 는 전체 데이터를 정렬하지 않는다.
* 각 파티션에서 상위 (또는 하위) n 개 요소를 가져온 후 이를 병합한 뒤 이 중 (상위 또는 하위) n개 요소를 반환한다.
* 이렇게 하여 `top` 과 `takeOrdered` 연산자는 훨씬 더 적은 양의 데이터를 네트워크로 전송하며,  `sortBy` 와 `take` 를 개별적으로 호출하는것보다 훨씬 빠르다.
* 하지만 `collect` 와 마찬가지로 모든 결과를 메모리로 가져오기 때문에 n 에 너무 큰값을 지정해서는 안된다.



### 4.3.3 데이터 그루핑

* **그루핑**
  * 데이터를 특정 기준에 따라 단일 컬렉션으로 집계하는 연산.
  * `aggregateByKey`, `groupByKey`(또는 `groupBy`), `combineByKey` 등

#### 4.3.3.1 groupByKey 나 groupBy 변환 연산자로 데이터 그루핑

* `groupByKey` 변환 연산자는 동일한 키를 가진 모든요소를 단일 키-값 쌍으로 모은 `Pair RDD` 를 반환한다.

 ```tex
  (A, 1)
  (A, 2)						 (A, (1,2))
  (B, 1)        =>  (B, (1,3))    
  (B, 3)						 (C, (1))
  (C, 1)
 ```

* `(K, Iterable[V])` 타입의 요소로 구성된다.
* `groupBy` 는 일반 RDD 에더 사용할 수 있고, 일반 RDD를 Pair RDD로 변환해 `groupByKey` 를 호출하는 것과 같은 결과를 만들 수 있다.
* `groupByKey` 는 각 키의 모든 값을 메모리로 가져와 메모리 리소스를 과다하게 사용하지 않도록 주의해야 한다.
* 만약 모든 값을 한번에 그루핑할 필요가 없으면 `aggregateByKey`,`reduceByKey`,  `foldByKey` 를 사용하는게 좋다.



#### 4.3.3.2 combineByKey 변환 연산자로 데이터 그루핑

* 이제 우리가 받은 마지막 요구사항인 **전일 판매 실적 통계** 를 `combineByKey` 를 사용하여 계산해보자.

* `combineByKey` 를 사용하기 위해선 3가지 커스텀 함수를 정의해 전달해야 한다. 그 중 두번째 함수는 **`Pair RDD` 에 저장된 값들을 결합 값으로 병합** 하고 세번째 함수는 **결합 값을 최종 결과로 병합**한다. 또한 메서드에 `Partitioner` 를 명시적으로 지정해야한다. 

* 마지막으로 `mapSideCombine` 플래그와 커스텀 `serializer` 를 선택인수로 지정할 수 있다.

  ```scala
  def combineByKey[C](createCombiner: V => C,
                     mergeValue: (C,V) => C,
                     mergeCombiners: (C,C) => C,
                     partitioner: Partitioner,
                     mapSideCombine: Boolean = true,
                     serializer: Serializer = null):RDD[(K, C)]
  ```

* 첫번째 함수인 `createCombiner` 는 각 파티션 별로 키의 첫번째 값(C) 에서 최초 결합 값(V)을 생성하는데 사용한다. 

* 두번째 전달하는 `mergeValue` 함수는 동일 파티션 내에서 해당 키의 다른 값을 결합 값에 추가로 병합하는데 사용한다.

* 세번째 함수인 `mergeCombiners` 는 여러 파티션의 결합 값을 최종 결과로 병합하는데 사용한다.

* 마지막 두 선택인자도 셔플링과 관련이 있다.

  * `mapSideCombine` 플래그(기본 값: true) 는 각 파티션의 결합 값을 계산하는 작업을 셔플링 단계 이전에 수행할지 지정한다.
    * 하지만 `combineByKey` 가 셔플링 과정을 거치지 않을 때는 결합 값을 파티션 안에서만 계산하기 때문에 이 인자도 사용되지 않는다.
  * 마지막으로 기본 `Serializer` 를 사용하고 싶지 않은 스파크 전문가는 커스텀 `Serializer` 를 메서드에 전달 할 수 있다. 

* 참고로 다른 그루핑 연산자(`aggregateByKey`,`reduceByKey`,  `foldByKey`, `groupByKey`)도 `combineByKey` 를 사용한다.

* 우리가 계속 사용하던 `transByCust` 는 `고객 ID` 와 `구매 기록` 을 묶어서 키-값 형태로 저장했지만 데이터를 고객별로 그루핑 하지 않았다.
* ```scala
  def createComb = (t:Array[String]) => { //결합 값을 생성하는 함수
    val total = t(5).toDouble // total은 구매금액
    val q = t(4).toInt	// q 는 구매수량
    (total/q, total/q, q, total) //(total/q) 은 상품 낱개 가격
  }
  
  
  def mergeVal: ((Double, Double, Int, Double),Array[String])=> 
  (Double, Double, Int, Double) = //결합 값과 값을 병합하는 함수
  { case((mn,mx,c,tot),t) => {
    val total = t(5).toDouble
    val q = t(4).toInt
    (scala.math.min(mn, total/q), scala.math.max(mx+total/q),c+q, tot+total) //구매수량과 구매 금액을 각각 결합해서 더함
  }}
  
  
  def mergeComb: ((Double, Double, Int, Double), (Double,Double,Int,Double)) =>
  (Double, Double, Int, Double) = //결합 값을 서로 병합하는 함수 
  { case((mn1, mx1, c1, tot1),(mn2, mx2,c2,tot2))=>
  (scala.math.min(mn1,mn2), scala.math.max(mx1, mx2), c1+c2,tot1+tot2)}
  
  
  val avgByCust = transByCust.combineByKey(createComb, mergeVal, mergeComb, new  org.apache.spark.HashPartitioner(transByCust.partitions.size)).mapValues({case(mn,mx,cnt,tot,tot/cnt)}) //튜플에 상품의 평균 가격을 추가한다.
  
  avgByCust.first() //값 확인
  
  ```



## 4.4 RDD 의존 관계

* RDD 의존관계는 RDD에 복원성을 부여하며, 스파크 잡 및 태스크 생성에도 영향을 미친다.

### 4.4.1 RDD 의존관계와 스파크 동작 매커니즘

* 스파크의 실행 모델은 **방향성 비순환 그래프** 에 기반한다.

  * 방향성 비순환 그래프란 간선의 방향을 따라 이동했을 때 같은 정점에 두번 이상 방문할 수 없도록 연결된 그래프를 의미한다.(즉, 순환하지 않는 그래프를 의미한다.)
  * RDD의 변환 연산자를 호출 할 때마다 새로운 정점(RDD) 과 새로운 간선(의존관계)이 생성된다.
  * 변환 연산자로 생성된 새 RDD가 이전  RDD 에 의존하므로 간선 방향은 **자식**  RDD 에서 **부모** RDD로 향한다. 이러한 RDD 의존 관계 그래프를 **RDD 계보** 라고한다.

* RDD 의 의존관계는 크게 두가지 기본유형으로 나눌 수 있다.

  * **좁은** 의존관계
    * 데이터를 다른 파티션으로 전송할 필요가 없는 변환 연산
    * 좁은 의존관계은 다시 **1대1 의존관계**와 **범위형 의존관계** 로 나타 낼 수 있다. 
      * 범위형 의존관계
        * 여러 부모  RDD 에 대한 의존관계를 하나로 결합한 경우로 `union` 변환 연산자만 해당
      * 1대1 의존관계
        * 셔플링이 필요하지 않은 모든 변환 연산자
  * **넓은** 의존관계
    * 셔플링을 수행할 때 형성
    * 참고로 조인하면 항상 셔플링이 수행된다.

* 우선 다음 코드를 이해해보자

  ```scala
  val list= List.fill(500)(scala.util.Random.nextInt(10))
  val listrdd = sc.parallelize(list,5)
  val pairs = listrdd.map(x => (x, x*x)) //RDD를 Pair RDD로 매핑한다.
  val reduced = pairs.reduceByKey((v1, v2)=>v1+v2) //각 키별로 RDD 값을 합산한다.
  val finalrdd = reduced.mapParitions( //RDD의 각 파티션을 매핑해 키-값 쌍 내용을 문자열로 구성한다.
  	iter => iter.map({case(k,v)=>"K="+k", V="+v})
  )
  finalrdd.collect()
  ```

  <img width="601" alt="image" src="https://user-images.githubusercontent.com/55227984/136753799-7212ba66-d3cb-447b-b755-c1a7ce267c92.png">

 * 참고로 다음그림에서 나타내는 것은 다음과 같다.

    * 둥근 사각형 : 각 RDD의 파티션
    * RDD 사이에 굵은 화살표 : RDD 를 생성한 변환연산
    * 꺾인 화살표 : RDD 계보 사슬을 이루는 RDD 의존 관계
       * `finalRDD` 는 `reduced` 에 의존하고, `reduced` 는 `pairs` 에 의존하며 `pairs` 는 `listrdd` 에 의존한다.
   * 옅은 직선 : 프로그램 실행중 발생하는 데이터 흐름
     * `map` 연산시 각 파티션이 다른 파티션과 교환할 필요가 없으므로 데이터의 흐름이 파티션 내로 한정되었다. 즉, 좁은(1대1) 의존관계를 형성한다.
     * `reduceByKey` 연산은 셔플링을 수행하며 넓은(또는 셔플) 의존관계를 형성한다.
       * 이는 `map` 변환 연산자가 `Partitioner` 를 제거하기 때문이다.
       * 다시말해 파티션 간의 데이터 이동을 볼 수 있다.
     * 스파크 쉘 에서도 `toDebugString` 메서드를 호출하여 위 끄림과 유사한 그래프정보를 텍스트형식으로 출력 받을 수 있다.

* 새로운 RDD 는 이전 RDD의 자식이 된다.



### 4.4.2 스파크의 스테이지와 태스크

* 스파크는 잡 하나를 여러 stage로 나눈다.

  <img width="577" alt="image" src="https://user-images.githubusercontent.com/55227984/136758210-192ac113-049f-416a-866f-069fbffb1b72.png">

* 우선 나눠진 형태를 보자

  * 1번 stage 는 셔플링으로 이어지는 변환연산들(`parallelize`,`map`, `reduceByKey`) 변환 연산들이 포함된다.
  * 1번 stage 결과는 중간 파일의 형태로 실행자 머신의 로컬 디스크에 저장된다.
  * 2번 stage는 중간 파일의 데이터를 적절한 파티션으로 읽은 후 두번쨰 `map` 변환연산자부터 마지막 `collect` 연산자 까지 실행한다.

* 스파크는 각 스테이지와 파티션별로 Task 를 생성해 실행자에 전달한다.

* 스테이지가 셔플링으로 끝나는 경우 이 단계의 태스크를 **셔플-맵** Task라 한다.

  * 스테이지의 모든 태스크가 완료되면 드라이버는 다음 스테이지의 Task를 생성해 실행자에 전달한다.

* 이 과정은 마지막 스테이지 결과를 드라이버로 반환할 때까지 계속한다. 마지막 stage에 생성된 Task를 **결과  Task** 라고 한다.

### 4.4.3 체크포인트로  RDD 계보 저장

* 변환 연산자를 계속 이어 붙이면 RDD 계보는 제약없이 길어진다.
* RDD를 안전하게 보관한다면 일부노드에 장애가 발생해도 유실된  RDD 조각을 처음부터 다시 계산할 필요가 없다.
* 장애발생 이전에 저장한 스냅샷을 사용해 나머지 계보를 다시 계산하면된다.
* 이 기능을 **체크 포인팅** 이라고 한다.
  * 체크포인팅을 실행하면 스파크는 데이터뿐만 아니라 RDD의 계보까지 모두 디스크에 저장한다.
  * 체크포인팅을 완료하면 해당  RDD 의존관계와 부모 RDD정보를 삭제한다. 다시 계산할 필요가 없기 때문이다.
  * 체크포인팅은 RDD 의 `checkpoint` 메소드를 호출한 후 실행할 수있다.
    * 하지만 먼저 `SparkContext.setChectpointDir` 에 데이터를 저장할 디렉토리 설정을 해야한다.
    * `HDFS` 디렉토리를 보통 설정하지만  로컬 디스크도 설정할 수있다.
  * `checkpoint` 메소드는 해당  RDD 잡이 실행되기 전에 호출해야하고, 체크포인팅이 실제로 완료하려면 `RDD`에 행동 연산자 등을 호출해 잡을 실행하고 RDD를 구체화 해야한다.

## 4.5 누적 변수와 공유 변수

* 누적변수
  * 여러 실행자가 공유하는 변수
  * 스파크 프로그램의 **전역상태**를 유지
  * 더하는 연산만 허용
* 공유 변수
  * **클러스터 노드가 공동**으로 사용할 수 있는 변수
  * 태스크 및 파티션이 공통으로 사용하는 데이터를 공유



### 4.5.1 누적 변수로 실행자에게 데이터 가져오기

* `SparkContext.accumulator(initalValue)` 를 호출해 생성한다.
* `sc.accumulator(initialValue, "accumulatorName")` 을 호출하면 누적변수이름을 지정할 수 있다. 
* 파이썬에서는 누적변수이름을 지정할 수 없다.
* `value` 메소드로 변수 값을 가져올 수 있다.
* 누적변수 값은 오직 드라이버만 참조 할 수 있다.(실행자가 누적변수 값에 접근하면 예외가 발생한다.)

```scala
scala > val acc = sc.accumulator(0, "acc name")
scala > val list = sc.parallelize(1 to 1000000)
scala > list.foreach(x => acc.add(1)) //실행자에서 수행하는 코드
scala > acc.value // 드라이버에서 수행하는 코드
---output---
res0:Int = 1000000

scala > list.foreach(x=>acc.value) //예외발생

```

* 실행자가 실제로 더하는 값의 타입과 누적 변수 값의 타입이 다를 경우 `Accumulable` 객체를 생성할 수 있다.
  * `Accumulable` 객체는 `SparkContext.accumulable(initialValue)` 를 호출 생성하며 , `Accumulator` 클래스와 마찬가지로 변수에 이름을 지정 할 수 있다. 
  * `Accumulable` 객체는 주로 커스텀 누적변수 역할을 한다.

#### 4.5.1.1 사용자 정의 누적 변수 작업

* `Accumulator` 값은 `AccumulatorParam` 객체를 사용해 누적한다. 

* `AccumulatorParam` 은 `accumulator` 함수를 호출하는 스코프 내에서 암시적으로 정의해야한다.

* 숫자형 타입의 누적변수를 사용할 땐 스파크가 암시적 `AccumulatorParam` 객체를 미리 제공하므로 별도의 객체를 정의할 필요가 없다.

* 커스텀 클래스를 누적변수로 사용하려면 별도의 커스텀 `AccumulatorParam`  객체를 암시적으로 정의한다.

* `AccumulatorParam` 을 정의하려면 다음 두 메서드를 구현해야한다.

  * `zero(initalValue: T)` : 실행자에 전달할 변수의 초깃값을 생성한다. 전역 초깃값도 동일하게 생성한다.
  * `addInPlace(v1: T, v2: T)` : 누적 값 두 개를 병합해야한다.

* `AccumulableParam` 에서는 다음 메서드를 추가로 구현해야한다.

  * `addAccumulator(v1: T, v2: V): T` : 누적 값에 새로운 값을 누적한다.

* 다음 예시를 통해 단일 `Accumulator` 객체를 사용하여 RDD 요소의 총합과 개수를 한꺼번에 집계하고 요소의 평균을 계산할 수 있다.

  ```scala
  val rdd = sc.parallelize(1 to 100)
  import org.apache.spark.AccumulableParam
  implicit object AvgAccParam extends AccumulableParam[(Int, Int), Int]{
    def zero(v:(Int, Int)) = (0, 0)
    def addInPlace(v1:(Int, Int), v2:(Int, Int)) = (v1._1+v2._1, v1._2 + v2._2)
    def addAccumulator(v1: (Int, Int), v2:Int) = (v1._1+1, v1._2+v2)
  }
  
  val acc = sc.accumulable((0,0)) //Accumulable 객체를 생성. 컴파일러는 자동으로 AvgAccParam 객체를 사용한다.
  rdd.foreach(x => acc+=x) //RDD의 모든 값을 누적 변수에 더한다.
  val mean = acc.value._2.toDouble / acc.value._1 // 누적 변수 값을 사용해 평균을 계산한다.
  
  ---output---
  mean -= 50.5
  ```

* AvgAccParam 객체는 각 실행자가 전달한 정수를 (개수, 합계) 튜플의 형태로 누적한다.

* `SparkContext.accumulableCollection()` 을 이용하면 따로 암시적 객체 정의할 필요 없이 가변 컬렉션에 값을 누적할 수 있다.

* 커스텀 Accumulator는 다양한 기능을 제공하지만 단순한 형태의 컬렉션 누적변수만 필요하다면 `accumulableCollection` 을 사용하는것이 더 편하다.

### 4.5.2 공유 변수로 실행자에 데이터 전송

* 실행자가 수정할 수 없다.
* 드라이버만이 공유 변수를 생성, 실행자에서는 읽기 연산만 가능
* 실행자 대다수가 대용량 데이터를 공통으로 사용할 땐 공유변수로 만드는 것이 좋다.
* 보통은 드라이버에서 생성한 변수를 Task 에서 사용하면 스파크는 이변수를 직렬화하고 Task와 함께 실행자로 전송한다.
* 하지만 드라이버 프로그램은 동일한 변수를 여러잡에 걸쳐 재사용할 수 있고 잡 하나를 수행할 떄도 여러 태스크가 동일한 실행자에 할당될 수 있으므로변수를 필요이상으로 여러번 직렬화해 네트워크 전송하는 상황이 발생할 수 있다.
* 이떈 단 한번만 전송하는 공유변수를 사용하는게 좋다.
* 공유변수 생성은 `SparkContext.broadcast(value)` 메서도르 생성한다.
* `value` 인수엔 직렬화 가능한 모든 종류의 객체를 전달 할 수 있다.
* 공유 변수 값을 참조 할 땐 항상  `value` 메소드를 사용해야 한다.
  * 그렇지 않고 공유변수에 직접 접근하면 스파크는 이 변수를 자동으로 직렬화하여 Task와 함께 전송한다.



####  4.5.2.1 공유 변수를 임시제거 또는 완전 삭제

* `destory` 를 사용하여 공유변수를 완전히 삭제할 수 있다.
* `unpersist` 메서도를 호출해 공유변수 값을 **캐시**에서 제거할 수 있다.
* 스파크는 공유변수의 모든 참조가 사라지면 자동으로  `unpersist` 를 호출한다.



#### 4.5.2.2 공유변수와 관련된 스파크 매개변수

* 공유변수 성능의 영향을 미치는 스파크 환경 매개변수다
  * `spark.broadcast.blockSize`
    * 공유 변수를 전송하는데 데이터 청크 크기를 설정한다.
    * 기본값(4096KB)을 그대로 유지하는게 좋다.
  * `spark.broadcast.compress`
    * 공유 변수를 전송하기 전에 데이터를 압축할지의 여부를 지정한다.
    * `spark.io.compression.codec` 에 지정된 코덱으로 압축한다.
  * `spark.python.worker.reuse`
    * 파이썬 공유 변수 성능에 영향을 주는 매개변수
    * 워커를 재상용하지 않으면 태스크 별로 공유변수를 전송해야한다.
    * 기본 값인 `true` 를 유지하는게 좋다.

공유변수**핵심**

* 대부분의 워커는 대규모 데이터를 공통으로 사용할 때는 공유변수를 사용해야 한다.
* 공유 변수 값에 접근할 때는 항상 `value` 메소드를 사용해야한다.

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
* 스파크의 조인 연산자는 이름이 같은 RDMBS 조인연산자와 동일 하게 동작한다.
* `cogroup` 은 여러 RDD 값을 키로 그룹핑하고 각 RDD 의 키별 값을 담은 `Iterable` 객체를 생성한후, 이  `Iterable` 객체의 배열을 `Pair RDD` 값으로 반환한다.
* `RDD` 의 테이터를 정렬하는데 `sortByKey` , `sortBy` , `repartitionAndSortWithPartition` 이있다.
* 스파크에서는 다양한  `Pair RDD` 변환 연산자로 데이터를 그루핑 할 수 있다.
* RDD 계보는 DAG 로 표현한다.
* 스파크는 **셔플링이 발생하는 지점**을 기준으로 스파크 잡 하나를  stage 여럿으로 나눈다.
* 체크포인팅을 `RDD` 계보를 저장할 수 있다.
* 누적 변수로 스파크 프로그램을 **전역 상태**로 유지할  수 있다.
* 공유 변수로 태스크 및 파티션이 공통으로 사용하는 데이터를 공유할 수 있다. 

## 참고 

* Scala val과 var의 차이
  * `val` 은 변경이 불가능 하지만 `var` 은 변경할 수 있다.
   <img width="263" alt="image" src="https://user-images.githubusercontent.com/55227984/136650722-2612e8fb-df79-4376-b1b5-971a809e70c5.png">
  
* `sc.parallelize()`

  * `parallelize()` 는 spark 에서 input data의 새로운 집합을 생성하라는 의미

  * ```scala
    sc.parallelize(data, 8) // data를 저장할때 8조각으로 쪼개서 메모리에 올리라는 의미
    												// data를 8개의 파티션으로 나눔
    ```
  
* Trait 이란?

  * `Java` 의 인터페이스와 비슷하다.

  * 주 생성자를 가질 수 없다.

    * ```scala
      trait Manager { // OK
      	def lookup(): Unit
      }
      
      trait Manager(val name: String){ //Compile Error
        ...
      }
      ```

  * `extends` 연산자를 사용하여 상속한다. 

    * ```scala
      trait TaskManager extends Manager {
        def run(body: => Unit):Unit = {
          println("TaskManager running")
        }
        
        override def lookup(): Unit = {
          println("TaskManger task lookup")
        }
      }
      ```

* `implicit` 이란?

  * 정의나 행위를 직접 언급 또는 기술하는 것이 아니라 당연시되어 내용을 생략하거나 합의하에 생략한다는 느낌 정도의 표현

  * `implicit Conversion`

    * ```scala
      class IntWrapper(number: Int){
        def square = number * number
      }
      
      implicit def toIntWrapper(n : Int) = new IntWrapper(n) // 만약 여기서 implicit 을 생략하면 당연히 오류
      
      2 square //4
      ```

    * ```scala
      def main():Unit = {
      	implicit val str2: String = "Brandon"
        implicit val str: String ="Hello Scala"
        
        def printlnSomething(implicit s: String):Unit = {
          println(s"*$s*")
        }
        
        printlnSomething
      }
      
      // 이런경우엔 오류가 생성되게 됨 왜냐하면 String 타입이 2개이기 때문에 ambiguous 하다는 오류가 발생함
      ```

  * `Ordering` 과 `Ordered` 의 차이

    * `Ordering` 은 자바의 `Comparator` 를 상속받아 만들어졌고 `Ordered` 는 자바의 `Comparable` 를 상속받아 만들어졌다.
      * 즉, `Ordered` 는 **자기 자신과 매개변수 객체를 비교** 하는 것이고 `Ordering` 은 **두 매개변수 객체를 비교** 하는 것이다.
      * `Ordered` 는 자기 자신과 파라미터로 들어오는 객체를 비교하는 것이고 `Ordering` 은 자기 자신의 상태가 어떻던 상관없이 파라미터로 들어온는 두 객체를 비교한다. 



  * `collect()` 란?

      * **RDD의 모든 데이터 요소 리턴** [Actions 함수]

        scala> data.collect()

        res21: Array[Int] = Array(1, 2, 3, 3)

        

   * Spark vs SQL

         * 
