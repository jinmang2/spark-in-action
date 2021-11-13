#  7장 MLlib로 더 똑똑해지자



7.1 머신러닝의 개요

7.2 스파크에서 선형대수 연산 수행

7.3 선형회귀

7.4 데이터 분석 및 준비 

7.5 선형 회귀 모델 학습 및 활용

7.6 알고리즘 정확도 극대화

7.7 알고리즘 성능 최적화 

7.8 요약



7장에서는 linear regression을 사용해 보스턴 주택가격 예측하는 예제를 구현한다. 

## 7.1 머신러닝의 개요

머신러닝 프로젝트는 다음 단계로 진행된다. 

![image-20211107183412545](C:\Users\Gachon\AppData\Roaming\Typora\typora-user-images\image-20211107183412545.png)

1. 데이터수집 

   가장 먼저 다양한 소스에서 대이터를 수집한다. 

   스파크를 사용하면 관계형 데이터베이스, CSV file 원격서비스, HDFS같은 분산 파일 시스템 등에서도 데이터를 가져올 수 있으며 스파크스트리밍으로 실시간 데이터를 받을수 있다, 

2. 데이터 정제 및 준비

3. 데이터 분석 및 특징 변수 추출

4. 모델훈련 

5. 모델평가

6. 모델 적용 

머신러닝 API를 사용해 모델을 학습하고 테스트 하는 작업은 전체 프로세스중 가장 짧으며 맨 마지막에 진행. 7~8장에선 주로 4단계 모델훈련과 5단계 모델평가를 다룬다. 

### 7.1.1 머신러닝의 정의

**머신러닝은 데이터를 학습하고, 데이터를 예측하는 알고리즘을 구성하고 연구하는 과학분야이다.**

### 7.1.2 머신러닝 알고리즘의 유형

 supervised learning 지도학습     데이터에 레이블있음

unsupervised learning 비지도학습   데이터에 레이블 없음 

#### 7.1.2.1

지도학습은 크게 회귀랑 분류로 나눔

회귀는 입력 변수를 바탕으로 연속된 출력의 값을 예측하는것

분류는 입력변수를 클래스 두개 이상으로 구분하는 것. 

**-> 너무 ML base 내용이라 SPARK내용만 추릴께요**

### 7.1.3 스파크를 활용한 머신러닝

2가지 장점

1. 스파크의 분산처리 능력을 이용해 매우 큰 규모의 데이터셋에서도 비교적 빠른 연산속도로 ML알고리즘을 훈련하고 적용할 수 있다. 

2. ML작업을 대부분 한곳에서 수행할 수 있는 통합 플랫폼을 제공 

   -> 스파크 내에서 데이터를 수집해 준비하고 다각도로 분석가능하고 이를 이용해 모델훈련과 평가에 활용하고 완성된 모델을 운영환경에 적용할 수있다. 

   이 모든 과정을 단일 API로 구현할 수 있다. 

스파크에는 가장 널리 활용되는 주요 ML 알고리즘을 분산방식으로 구현, 지원알고리즘도 늘어나고있음. 

스파크 머신러닝의 핵심 API는 UC 버클리의 MLBase 프로젝트를 기반으로 한 MLlib이다. 

스파크 ML은 머신러닝 파이프라인 기능을 제공, ML과 관련된 모든 연산 작업을 시퀀스 하나로 모아 마치 단일 연산처럼 한번에 처리 가능하다. 

스파크는 선형대수 연산을 최적화 할 수 있는 여러 하위레벨 라이브러리를 사용한다. (스칼라, 자바는 Breeze. jblas를사용, 파이썬스파크는 Numpy를 사용)

Spark MLlib 공식문서 http://spark.apache.org/docs/latest/mllib-guide.html#dependencies



## 7.2 스파크에서 선형 대수 연산 수행 

스파크는 Breeze. jblas(파이썬스파크는 Numpy를 사용)를 로컬환경의 선형대수 연산에 사용, 분산환경의 선형대수 연산은 스파크 자체적으로 구현. 

### 7.2.1 로컬벡터와 로컬행렬

스파크의 ```org.apache.spark.mllib.linalg``` 패키지는 로컬벡터와 로컬행렬을 제공한다. 

이를 활용해 스파크의 선형대수  API를 알아본다. 

로컬모드로 스파크셸을 실행하려면 리눅스셸에서 ```spark-shell --master local[*]```을 입력한다. 

#### 7.2.1.1 로컬벡터 생성

스파크의 로컬 벡터는 ```DenseVector``` 클래스와 ```SparseVector``` 클래스로 만들 수 있다. 

이 클래스는 공통 인터페이스를 상속받아 정확히 동일한 연산을 지원한다. 

로컬벡터는 주로 ```Vectors```클래스의 ```dense```나 ```sparse``` 메서드로 생성한다. 

```dense```메서드는 모든 원소값을 인라인 인수로 전달하거나 원소 값의 배열을 전달한다. 

```sparse``` 메서드를 호출하려면 벡터크기, 위치배열, 값배열을 지정해야한다. 

다음 코드로 생성한 벡터 세개 ```dvq, dv2, sv```는 동일한 원소를 포함하므로 수학적으로 동일하다. 

```scala
import org.apache.spark.mllib.linalg.{Vectors, Vector}

scala> val dv1 = Vectors.dense(5.0,6.0,7.0,8.0)
dv1: org.apache.spark.mllib.linalg.Vector = [5.0,6.0,7.0,8.0]

scala> val dv2 = Vectors.dense(Array(5.0,6.0,7.0,8.0))
dv2: org.apache.spark.mllib.linalg.Vector = [5.0,6.0,7.0,8.0]

scala> val sv = Vectors.sparse(4, Array(0,1,2,3), Array(5.0,6.0,7.0,8.0))
sv: org.apache.spark.mllib.linalg.Vector = (4,[0,1,2,3],[5.0,6.0,7.0,8.0])

scala> dv2(2)          ##벡터 내 특정 위치의 원소 가져오기
res0: Double = 7.0

scala> dv1.size        ## 벡터 크기 확인
res1: Int = 4

scala> dv2.toArray    ## 모든 원소를 배열 형태로 가져옴
res2: Array[Double] = Array(5.0, 6.0, 7.0, 8.0)
```

#### 7.2.1.2 로컬 벡터의 선형 대수 연산. 

로컬 벡터의 선형 대수 연산은 ```Breeze```라이브러리를 사용해 수행할 수 있다. 

스파크의 로컬벡터 및 로컬행렬 클래스에는 ```toBreeze```함수가 있지만 ```private```로 선언되서 사용할 수 없다. 

따라서 스파크 벡터를 ```Breeze```의 클래스로 변환하는 함수를 직접 만들 수 있다. 

```scala
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector}
import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV}
def toBreezeV(v:Vector):BV[Double] = v match {
    case dv:DenseVector => new BDV(dv.values)
    case sv:SparseVector => new BSV(sv.indices, sv.values, sv.size)
}

## 이제 Breeze 라이브러리를 사용해 벡터의 덧셈이나 내적을 계산할 수 있다. 

scala> toBreezeV(dv1) + toBreezeV(dv2)
res3: breeze.linalg.Vector[Double] = DenseVector(10.0, 12.0, 14.0, 16.0)

scala> toBreezeV(dv1).dot(toBreezeV(dv2))
res4: Double = 174.0
```

#### 7.2.1.3 로컬 밀집 행렬 생성

```Vector``` 클래스와 마찬가지로 ```Matrices```클래스에서도 ```dense```나 ```sparse```메서드를 사용해 행렬을 생성할 수 있다. 

```dense```메서드에는 행갯수, 열갯수, 데이터를 담은 배열을 전달한다.  왼쪽부터 오른쪽가지 순차적으로 채우도록 구성.
$$
M = \begin{bmatrix}
5 & 0 & 1 \\
0 & 3 & 4
\end{bmatrix}
$$
이 행렬을 ```DenseMatrix```로 생성해 보자. 

```scala
import org.apache.spark.mllib.linalg.{DenseMatrix, SparseMatrix, Matrix, Matrices}
import breeze.linalg.{DenseMatrix=>BDM, CSCMatrix=>BSM, Matrix=>BM}
val dm = Matrices.dense(2,3,Array(5.0, 0.0, 0.0, 3.0, 1.0, 4.0))
dm: org.apache.spark.mllib.linalg.Matrix =
5.0  0.0  1.0
0.0  3.0  4.0
```

```Matrices``` 객체는 단위행렬, 대각행렬, 영행렬 1행렬 을 쉽게 생성할 수 있는 메서드를 제공. 

```eye(n)```메서드는 크기가 n x n 인 밀집 단위행렬을 생성. ```speye```도 유사하게 희소 단위행렬을 생성. 

```ones(m,n)```은 1행렬, ```zeros(m,n)```은 0행렬 생성.  ```diag```는 vector를 인수로 받아 대각행렬을 생성. 

```rand, randn```은 0~1 사이의 난수를 채운 ```DenseMatrix```를 생성. 

```rand```는 균등분포 ```randn```는 정규분포 생성 -> ```DenseMatrix```

```sprand```는 균등분포 ```sprandn```는 정규분포 생성 -> ```SparseMatrix```



#### 7.2.1.4 로컬  희소 행렬 생성. 

희소행렬을 만드는 것은 밀집행렬 보다 복잡하다. 

```sparse``` 메서드에 행과 열의 갯수를 전달하는것까지 동일. 하지만 원소값을 CSC포멧으로 전달해야 한다. 

CSC포맷은 열지정, 행위치배열, 원소배열로 구성. 원소 배열에는 희소 행렬의 각 원소를 저장하며 행 위치 배열에는 원소 배열의 각 요소가 위치할 행 번호를 저장한다. 열 지정 배열에는 같은 열에 위치한 원소들의 범위를 저장한다. 
$$
M = \begin{bmatrix}
5 & 0 & 1 \\
0 & 3 & 4
\end{bmatrix}
$$
전의 행렬을 CSC포멧으로 표현하면

```scala
colPtrs = [0 1 2 4], rowIndices = [0 1 0 1], elements = [5 3 1 4]
```

이를 활용해서 ```SparseMatrix``` 객체를 생성할 수 있다. 

```toDense, toSparse```메서드를 사용해 ```SparseMatrix, DenseMatrix```로 변환할 수 있다. 하지만 먼저 ```dense, sparse```메서드는 matrix를 반환함으로 이를 먼저 적적한 클래스 타입으로 변환해야 한다. 

```scala
scala> import org.apache.spark.mllib.linalg. {DenseMatrix, SparseMatrix}
scala> sm.asInstanceOf[SparseMatrix].toDense
res0: org.apache.spark.mllib.linalg.DenseMatrix =
5.0  0.0  1.0
0.0  3.0  4.0
scala> dm.asInstanceOf[DenseMatrix].toSparse
res3: org.apache.spark.mllib.linalg.SparseMatrix =
2 x 3 CSCMatrix
(0,0) 5.0
(1,1) 3.0
(0,2) 1.0
(1,2) 4.0
```

#### 7.2.1.5 로컬행렬의 선형 대수 연산.

```scala
scala> dm(1,1)
res4: Double = 3.0

scala> dm.transpose
res5: org.apache.spark.mllib.linalg.Matrix =
5.0  0.0
0.0  3.0
1.0  4.0
```

여러 편리한 로컬 행렬 연산을 사용하려면 스파크의 행렬 객체를 Breeze의 행렬 객체로 변환해야 한다. 



### 7.2.2 분산행렬 

대규모 데이터셋에 ML 알고리즘을 적용하려면 분산행렬이 필요하다. 분산행렬은 여러 머신에 걸쳐 저장할 수 있고 대량의 행과 열로 구성할 수 있다. 분산 행렬은 행과 열의 번호에 int가 아닌 long타입을 사용한다. 

스파크의 ```org.apache.spark.mllib.linalg.distributed```에는 네가지 유형의 분산 행렬을 제공한다. 

#### 7.2.2.1 RowMatrix

```RowMatrix```는 행렬의 각행을 vector로 저장해 RDD로 구성한다. 이 RDD는 RowMatrix클래스의 rows 멤버 변수로 가져올 수 있다. 

또 행렬이 가진 행 갯수와 열갯수는 각각 numRows와 numCols메서드로 가져올 수 있다. 

또 multiply 메서드를 사용해 RowMatrix에 로컬 행렬을 곱할 수 있다. (새로운 RowMatrix를 결과로 반환한다. ) 

이외에도 RowMatrix는 다른 분산 행렬 클래스에서는 사용할 수 없는 여러 유용한 메서드를 제공한다. 

스파크는 다른 분산 행렬 클래스 -> RowMatrix클래스로 변환하는 toRowMatirx를 제공. 

하지만 RowMatrix 클래스 -> 다른분산 클래스로 변환하는것은 없다 .

#### 7.2.2.2 IndexedRowMatrix

IndexedRowMatrix는 IndexedRow객체의 요소로 구성된 RDD 형태로 행렬을 저장한다. 

이 RDD의 각요소 즉 IndexedRow 객체는 행의 원소들을 담은 vector 객체와 이 행의 행렬 내 위치를 저장한다. 스파크에서는 RowMatrix를 IndexedRowMatrix로 변환하는 메서드를 제공하지 않지만 간단하게 변환할 수 있다.

```scala
scala> import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix
scala> import org.apache.spark.mllib.linalg.distributed.IndexedRow
scala> val rmind = new IndexedRowMatrix(rm.rows.zipWithIndex().map(x => IndexedRow(x._2, x._1)))   ### -> 동작 X rm 변수 선언 X 
```

#### 7.2.2.3 CoordinateMatrix

CoordinateMatrix는 MatrixEntry객체의 요소로 구성된 RDD형태로 행렬값을 나누어 저장한다. 

이 RDD의 각 요소 즉 MatrixEntry는 개별 원소값과 해당 원소의 행렬 내 위치(i, j)를 저장한다.

하ㅏ지만 이 방법으로는 데이터를 효율적으로 저장할수 없으므로 CoordinateMatrix는 오직 희소행렬을 저장할때만 사용해야 한다.

밀집 행렬을 CoordinateMatrix로 저장하면 메모리를 과다하게 사용할 수 밖에 없다.

#### 7.2.2.4 BlockMatrix

BlockMatrix는 다른 분산 행렬 클랫그와 달리 분산 행렬 간 덧셈 및 곱셈 연산을 지원한다. BlockMatrix 는 ((i,j), matrix)의 튜플의 RDD형태로 행렬을 저장한다.

BlockMatrix는 행렬을 블록 여러개로 나누고 각 블록의 원소들을 로컬 행렬의 형태로 저장한 후 이 블로그이 전체 행렬 내 위치와 로컬 행렬 객체를 튜플로 구성한다. 

BlockMAtrix의 validate메서드로 모든 블록 크기가 동일한 지 검사할 수 있다. 

#### 7.2.2.5 분산행렬의 선형대수 연산.

스파크는 분산행렬의 선형 대수 연산을 제한적으로 제공하므로 나머지 연산은 따로 구현해야 한다. 

예를들어 두 분산 행렬을 요소단위로 더하는 연산이나 행렬곱 연산은 BlockMatrix행렬만 제공한다.

대규모 행렬에서 이를 효율적으로 처리할수 있는것은 BlockMatrix가 유일

전치 행렬을 계산하는 Transpose메서드 또한 CoordinateMatrix와 BlockMatrix만 제공. 다른 행렬 연산은 별도로 구현. 

## 7.3 선형회귀 

-> 선형회귀는 넘어갈게요!

## 7.4 데이터 분석 및 준비

이 장에서는 주택 데이터셋을 이용한 선형회귀 문제를 풀것이다. 

데이터는 깃헙에서 받아서 사용.

```scala
scala> import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vectors

scala> val housingLines = sc.textFile("first-edition/ch07/housing.data", 6)
housingLines: org.apache.spark.rdd.RDD[String] = first-edition/ch07/housing.data MapPartitionsRDD[1] at textFile at <console>:35

scala> val housingVals = housingLines.map(x => Vectors.dense(x.split(",").map(_.trim().toDouble)))
housingVals: org.apache.spark.rdd.RDD[org.apache.spark.mllib.linalg.Vector] = MapPartitionsRDD[2] at map at <console>:36

## 파싱한 데이터를 vector객체에 담음. 
```

### 7.4.1 데이터 분포 분석

먼저 다변량 통계를 계산해 데이터셋을 파악해보자!

![image-20211112183507694](C:\Users\Gachon\AppData\Roaming\Typora\typora-user-images\image-20211112183507694.png)

```scala
### RowMatrix로 객체 생성하고 통계값 계산. 
scala> import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.distributed.RowMatrix

scala> val housingMat = new RowMatrix(housingVals)
housingMat: org.apache.spark.mllib.linalg.distributed.RowMatrix = org.apache.spark.mllib.linalg.distributed.RowMatrix@555cc0ba

scala> val housingStats = housingMat.computeColumnSummaryStatistics()
housingStats: org.apache.spark.mllib.stat.MultivariateStatisticalSummary = org.apache.spark.mllib.stat.MultivariateOnlineSummarizer@6683cf7a

### Statistics 사용해도 결과 동일!
scala> import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.stat.Statistics

scala> val housingStats = Statistics.colStats(housingVals)
housingStats: org.apache.spark.mllib.stat.MultivariateStatisticalSummary = org.apache.spark.mllib.stat.MultivariateOnlineSummarizer@5d34e3b3
```

결과로 반환된 MultivariateOnlineSummarizer 객체는 행렬의 각 열별 mean, max, min, normL1, normL2, variance 메서드를 제공.

```scala
scala> housingStats.min
res0: org.apache.spark.mllib.linalg.Vector = [0.00632,0.0,0.46,0.0,0.385,3.561,2.9,1.1296,1.0,187.0,12.6,0.32,1.73,5.0]
```

### 7.4.2 열 코사인 유사도 분석.

```scala
## RowMatrix객체에서 열 코사인 유사도를 가져옴
scala> val housingColSims = housingMat.columnSimilarities()
housingColSims: org.apache.spark.mllib.linalg.distributed.CoordinateMatrix = org.apache.spark.mllib.linalg.distributed.CoordinateMatrix@555ddfad

## 깃에서 toBreezeD 메서드 복붙!
import org.apache.spark.mllib.linalg.distributed.{RowMatrix, CoordinateMatrix, BlockMatrix, DistributedMatrix, MatrixEntry}
def toBreezeD(dm:DistributedMatrix):BM[Double] = dm match {
    case rm:RowMatrix => {
      val m = rm.numRows().toInt
       val n = rm.numCols().toInt
       val mat = BDM.zeros[Double](m, n)
       var i = 0
       rm.rows.collect().foreach { vector =>
         for(j <- 0 to vector.size-1)
         {
           mat(i, j) = vector(j)
         }
         i += 1
       }
       mat
     }
    case cm:CoordinateMatrix => {
       val m = cm.numRows().toInt
       val n = cm.numCols().toInt
       val mat = BDM.zeros[Double](m, n)
       cm.entries.collect().foreach { case MatrixEntry(i, j, value) =>
         mat(i.toInt, j.toInt) = value
       }
       mat
    }
    case bm:BlockMatrix => {
       val localMat = bm.toLocalMatrix()
       new BDM[Double](localMat.numRows, localMat.numCols, localMat.toArray)
    }
}
### printMat도 복붙!
def printMat(mat:BM[Double]) = {
   print("            ")
   for(j <- 0 to mat.cols-1) print("%-10d".format(j));
   println
   for(i <- 0 to mat.rows-1) { print("%-6d".format(i)); for(j <- 0 to mat.cols-1) print(" %+9.3f".format(mat(i, j))); println }
}

printMat(toBreezeD(housingColSims)) 
```

실행결과 

![image-20211112185130979](C:\Users\Gachon\AppData\Roaming\Typora\typora-user-images\image-20211112185130979.png)

### 7.4.3 공분산 행렬 계산

```scala
### 이거도 깃에서 복붙! 
def toBreezeM(m:Matrix):BM[Double] = m match {
    case dm:DenseMatrix => new BDM(dm.numRows, dm.numCols, dm.values)
    case sm:SparseMatrix => new BSM(sm.values, sm.numCols, sm.numRows, sm.colPtrs, sm.rowIndices)
}

val housingCovar = housingMat.computeCovariance()
printMat(toBreezeM(housingCovar))
```

결과 

![image-20211112185900020](C:\Users\Gachon\AppData\Roaming\Typora\typora-user-images\image-20211112185900020.png)

스파크에서는 corr도 지원하는데 책에서는 설명하지 않음

### 7.4.4 레이블 포인트로 변환

선형회귀에 사용할 데이터를 준비하는단계

데이터셋의 각 예제를 ```LabeledPoint```구조체에 담아야 한다. ```LabeledPoint```는 **거의 모든 ML알고리즘에 사용된다!**

목표변수값과 특징 변수 벡터로 구성. 

전에는 모든벡터를 Vector에 담은 housingVals와 housingMat RowMatrix객체를 사용했지만 이제는 레이블을 특징 변수들과 분리해야한다. 

```scala
### housingVals RDD를 변환하고 레이블과 다른 변수들을 분리.  레이블은 마지막 컬럼. 

scala> import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LabeledPoint

scala> val housingData = housingVals.map(x => { val a = x.toArray; LabeledPoint(a(a.length-1), Vectors.dense(a.slice(0, a.length-1))) })
housingData: org.apache.spark.rdd.RDD[org.apache.spark.mllib.regression.LabeledPoint] = MapPartitionsRDD[28] at map at <console>:38
```

### 7.4.5 데이터 분할

train, val 로 분할해야한다. 스파크에서는 RDD에 내장된 ```randomSplit```메서드를 사용해 쉽게 데이터를 분할할 수 있다. 

```scala
scala> val sets = housingData.randomSplit(Array(0.8, 0.2))
sets: Array[org.apache.spark.rdd.RDD[org.apache.spark.mllib.regression.LabeledPoint]] = Array(MapPartitionsRDD[29] at randomSplit at <console>:38, MapPartitionsRDD[30] at randomSplit at <console>:38)

scala> val housingTrain = sets(0)
housingTrain: org.apache.spark.rdd.RDD[org.apache.spark.mllib.regression.LabeledPoint] = MapPartitionsRDD[29] at randomSplit at <console>:38

scala> val housingValid = sets(1)
housingValid: org.apache.spark.rdd.RDD[org.apache.spark.mllib.regression.LabeledPoint] = MapPartitionsRDD[30] at randomSplit at <console>:38
```

### 7.4.6 특징변수 스케일링 및 평균 정규화

변수들을 스케일링을 통해 데이터 범위를 비슷한 크기로 조정. 

평균 정규화는 평균이 0에 가깝도록 데이터를 옮기는 작업

이를 한번에 실행하려면 스파크의 ```StandardScaler```객체가 필요하다. 

```scala
scala> import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.feature.StandardScaler

scala> val scaler = new StandardScaler(true, true).fit(housingTrain.map(x => x.features))
scaler: org.apache.spark.mllib.feature.StandardScalerModel = org.apache.spark.mllib.feature.StandardScalerModel@12a21500

### StandardScaler는 데이터의 요약통계를 찾아서 이를 작업에 활용. 
### train, val에 적용. 

scala> val trainScaled = housingTrain.map(x => LabeledPoint(x.label, scaler.transform(x.features)))
trainScaled: org.apache.spark.rdd.RDD[org.apache.spark.mllib.regression.LabeledPoint] = MapPartitionsRDD[37] at map at <console>:41

scala> val validScaled = housingValid.map(x => LabeledPoint(x.label, scaler.transform(x.features)))
validScaled: org.apache.spark.rdd.RDD[org.apache.spark.mllib.regression.LabeledPoint] = MapPartitionsRDD[38] at map at <console>:41
```

선형회귀에 사용할 데이터셋 준비완료. 

## 7.5 선형 회귀 모델 학습 및 활용

스파크의 선형 회귀 모델은 ```org.apache.spark.mllib.regression```패키지 안의 ```LinearRegressionModel``` 클래스에 구현되어 있다. 

이 클래스의 객체는 학습이 완료된 선형 회귀 모델의 매개변수가 저장된다 .

이 객체의 ```predict```메서드에 vector타입의 개별 데이터 예제를 전달하면 목표 레이블을 예측할 수 있다. 

``` LinearRegressionModel```객체는 선형회귀모델의 학습 알고리즘을 구현한 ```LinearRegressionWithSGD```클래스로 만들 수 있다. 

이를 사용하려면 ```LinearRegressionWithSGD``` 클래스의 train을 호출하면 된다. 다른 MLlib랑 동일! 

```scala
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
val alg = new LinearRegressionWithSGD()   ## 객체 촉디화
alg.setIntercept(true)                    ## Y절편을 학습하도록 설정
alg.optimizer.setNumIterations(200)       ## 반복 실행횟수를 설정
trainScaled.cache()						## 입력데이터 캐시 -> 중요
validScaled.cache()
val model = alg.run(trainScaled)		## 모델 훈련 시작. 
```

### 7.5.1 목표변수값 예측

모델 학습을 완료하면 검증 데이터셋의 각 요소별로 predict 메서드를 실행해 목표 변수값을 예측할 수 있다. 

검증 데이터셋의 각 요소는 목표 변수를 포함한 Labeled Point객체이지만 predict메서드에는 특징 변수들만 전달한다. 

또 실제 레이블과 예측 결과를 나란히 비교할 수 있도록 함께 저장해야한다. 

검증데이터셋의 ```LabeledPoint```를 실제 목표 변수값과 예측값의 튜플로 매칭

```scala
val validPredicts = validScaled.map(x => (model.predict(x.features), x.label))
validPredicts.collect()   ## 결과보기 
val RMSE = math.sqrt(validPredicts.map{case(p,l) => math.pow(p-l,2)}.mean())   ## RMSE 사용
```

### 7.5.2 모델 성능 평가

회귀 모델의 성능을 평가하는 방법은 RMSE외에도 여러개가 있다. ```RegressionMetrics```클래스를 사용해 여러 평가방법을 사용할 수 있다. 

```scala
import org.apache.spark.mllib.evaluation.RegressionMetrics
val validMetrics = new RegressionMetrics(validPredicts)
validMetrics.rootMeanSquaredError     ## RMSE
validMetrics.meanSquaredError 		## MSE
```

### 7.5.3 모델 매개변수 해석

각 가중치를 조회하는법

```scala
println(model.weights.toArray.map(x => x.abs).zipWithIndex.sortBy(_._1).mkString(", "))
```

7.5.4 모델 저장 및 불러오기. 

스파크는 학습된 모델을 ```Parquet```파일 포맷으로 파일 시스템에 저장하고 추후 다시 로드할 수 있는 방법을 제공.

스파크 MLlib모델들은 대부분 save메서드로 저장할 수 있다. 이 메서드에는 ```SparkContext```인스턴스와 저장할 파일 결로를 저장. 

```scala
model.save(sc, "hdfs:///path/to/saved/model")
```

스파크는 지정된 경로에 모델을 ```Parquet```포멧으로 저장

모델 을 로드하려면 ```load```메서드에 ```SparkContext```인스턴스와 모델의 경로를 전달. 

```scala
import org.apache.spark.mllib.regression.LinearRegressionModel
val model = LinearRegressionModel.load(sc, "hdfs:///path/to/saved/model")
```

## 7.6 알고리즘 정확도 극대화

Gradient desent의 lr과 epoch 의 최적값을 찾아야 한다. 

### 7.6.1 적절한 이동거리와 반복 횟수를 찾는법. 

깃에 코드 복붙!

```scala
import org.apache.spark.rdd.RDD
def iterateLRwSGD(iterNums:Array[Int], stepSizes:Array[Double], train:RDD[LabeledPoint], test:RDD[LabeledPoint]) = {
  for(numIter <- iterNums; step <- stepSizes)
  {
    val alg = new LinearRegressionWithSGD()
    alg.setIntercept(true).optimizer.setNumIterations(numIter).setStepSize(step)
    val model = alg.run(train)
    val rescaledPredicts = train.map(x => (model.predict(x.features), x.label))
    val validPredicts = test.map(x => (model.predict(x.features), x.label))
    val meanSquared = math.sqrt(rescaledPredicts.map({case(p,l) => math.pow(p-l,2)}).mean())
    val meanSquaredValid = math.sqrt(validPredicts.map({case(p,l) => math.pow(p-l,2)}).mean())
    println("%d, %5.3f -> %.4f, %.4f".format(numIter, step, meanSquared, meanSquaredValid))
    //Uncomment if you wish to see weghts and intercept values:
    //println("%d, %4.2f -> %.4f, %.4f (%s, %f)".format(numIter, step, meanSquared, meanSquaredValid, model.weights, model.intercept))
  }
}

### 위의 함수는 반복횟수의 값을 여러개 담은 배열과 이동거리 값을 담은 배열, train_set RDD, val_set RDD를 인수로 받음. 
그리고 이 배열에 담긴 이동거리와 반복횟수의 조합별로 train, val에서 계산한 RMSE지표를 출력. 

iterateLRwSGD(Array(200, 400, 600), Array(0.05, 0.1, 0.5, 1, 1.5, 2, 3), trainScaled, validScaled)
// Our results:
// 200, 0.050 -> 7.5420, 7.4786
// 200, 0.100 -> 5.0437, 5.0910
// 200, 0.500 -> 4.6920, 4.7814
// 200, 1.000 -> 4.6777, 4.7756
// 200, 1.500 -> 4.6751, 4.7761
// 200, 2.000 -> 4.6746, 4.7771
// 200, 3.000 -> 108738480856.3940, 122956877593.1419
// 400, 0.050 -> 5.8161, 5.8254
// 400, 0.100 -> 4.8069, 4.8689
// 400, 0.500 -> 4.6826, 4.7772
// 400, 1.000 -> 4.6753, 4.7760
// 400, 1.500 -> 4.6746, 4.7774
// 400, 2.000 -> 4.6745, 4.7780
// 400, 3.000 -> 25240554554.3096, 30621674955.1730
// 600, 0.050 -> 5.2510, 5.2877
// 600, 0.100 -> 4.7667, 4.8332
// 600, 0.500 -> 4.6792, 4.7759
// 600, 1.000 -> 4.6748, 4.7767
// 600, 1.500 -> 4.6745, 4.7779
// 600, 2.000 -> 4.6745, 4.7783
// 600, 3.000 -> 4977766834.6285, 6036973314.0450
```

### 7.6.2 고차다항식 추가

$$
h(x) = w_0x^3 + w_1x^2+ w_2x + w_3
$$

고차 다항식을 추가하면 성능이 오를수 있다. 

addHighPols 함수 정의 깃에 있음!

```scala
def addHighPols(v:Vector): Vector =
{
  Vectors.dense(v.toArray.flatMap(x => Array(x, x*x)))
}
val housingHP = housingData.map(v => LabeledPoint(v.label, addHighPols(v.features)))

### housingHP RDD에는 housingHP RDD의 LabeledPoint를 이차 다항식으로 확장한 벡터를 저장. 특징변수는 13개에서 26개가 됨. 
housingHP.first().features.size -> 26

### 다시 데이터셋를 분할 후 스케일링. 

val setsHP = housingHP.randomSplit(Array(0.8, 0.2))
val housingHPTrain = setsHP(0)
val housingHPValid = setsHP(1)
val scalerHP = new StandardScaler(true, true).fit(housingHPTrain.map(x => x.features))
val trainHPScaled = housingHPTrain.map(x => LabeledPoint(x.label, scalerHP.transform(x.features)))
val validHPScaled = housingHPValid.map(x => LabeledPoint(x.label, scalerHP.transform(x.features)))
trainHPScaled.cache()
validHPScaled.cache()

### 그후 결과를 보면 
iterateLRwSGD(Array(200, 400), Array(0.4, 0.5, 0.6, 0.7, 0.9, 1.0, 1.1, 1.2, 1.3, 1.5), trainHPScaled, validHPScaled)
// Our results:
// 200, 0.400 -> 4.5423, 4.2002
// 200, 0.500 -> 4.4632, 4.1532
// 200, 0.600 -> 4.3946, 4.1150
// 200, 0.700 -> 4.3349, 4.0841
// 200, 0.900 -> 4.2366, 4.0392
// 200, 1.000 -> 4.1961, 4.0233
// 200, 1.100 -> 4.1605, 4.0108
// 200, 1.200 -> 4.1843, 4.0157
// 200, 1.300 -> 165.8268, 186.6295
// 200, 1.500 -> 182020974.1549, 186781045.5643
// 400, 0.400 -> 4.4117, 4.1243
// 400, 0.500 -> 4.3254, 4.0795
// 400, 0.600 -> 4.2540, 4.0466
// 400, 0.700 -> 4.1947, 4.0228
// 400, 0.900 -> 4.1032, 3.9947
// 400, 1.000 -> 4.0678, 3.9876
// 400, 1.100 -> 4.0378, 3.9836
// 400, 1.200 -> 4.0407, 3.9863
// 400, 1.300 -> 106.0047, 121.4576
// 400, 1.500 -> 162153976.4283, 163000519.6179

## 반복횟수 늘려보자
iterateLRwSGD(Array(200, 400, 800, 1000, 3000, 6000), Array(1.1), trainHPScaled, validHPScaled)
//Our results:
// 200, 1.100 -> 4.1605, 4.0108
// 400, 1.100 -> 4.0378, 3.9836
// 800, 1.100 -> 3.9438, 3.9901
// 1000, 1.100 -> 3.9199, 3.9982
// 3000, 1.100 -> 3.8332, 4.0633
// 6000, 1.100 -> 3.7915, 4.1138

## RMSE가 증가했다. 
```

### 7.6.3 편향-분신 상충 관계와 모델의 복잡도

-> 오버피팅 말하는것

```scala
iterateLRwSGD(Array(10000, 15000, 30000, 50000), Array(1.1), trainHPScaled, validHPScaled)
// Our results:
// 10000, 1.100 -> 3.7638, 4.1553
// 15000, 1.100 -> 3.7441, 4.1922
// 30000, 1.100 -> 3.7173, 4.2626
// 50000, 1.100 -> 3.7039, 4.3163

### train RMSE은 오르는데 val RMSE 은 떨어진다! 
```

### 7.6.4 잔차 차트 그리기

그래프 그리는건데 책에서는 설명 X

### 7.6.5 일반화를 통한 과적합 방지

```L1```을 통한 ```Lasso``` ,``` L2```를 통한 ```Ridge```를 사용한다.

스파크에는 ```LinearRegressionWithSGD.optimizer```객체의 ```regParam```, ```updater``` 설정을 바꿔주거나 ```LassoWithSGD```, ```RidgeRegressionWithSGD```를 사용 

사용하는법  (라소랑 릿지 코드 복붙!)

```scala
def iterateRidge(iterNums:Array[Int], stepSizes:Array[Double], regParam:Double, train:RDD[LabeledPoint], test:RDD[LabeledPoint]) = {
  import org.apache.spark.mllib.regression.RidgeRegressionWithSGD
  for(numIter <- iterNums; step <- stepSizes)
  {
    val alg = new RidgeRegressionWithSGD()
    alg.setIntercept(true)
    alg.optimizer.setNumIterations(numIter).setRegParam(regParam).setStepSize(step)
    val model = alg.run(train)
    val rescaledPredicts = train.map(x => (model.predict(x.features), x.label))
    val validPredicts = test.map(x => (model.predict(x.features), x.label))
    val meanSquared = math.sqrt(rescaledPredicts.map({case(p,l) => math.pow(p-l,2)}).mean())
    val meanSquaredValid = math.sqrt(validPredicts.map({case(p,l) => math.pow(p-l,2)}).mean())
    println("%d, %5.3f -> %.4f, %.4f".format(numIter, step, meanSquared, meanSquaredValid))
  }
}
def iterateLasso(iterNums:Array[Int], stepSizes:Array[Double], regParam:Double, train:RDD[LabeledPoint], test:RDD[LabeledPoint]) = {
  import org.apache.spark.mllib.regression.LassoWithSGD
  for(numIter <- iterNums; step <- stepSizes)
  {
    val alg = new LassoWithSGD()
    alg.setIntercept(true).optimizer.setNumIterations(numIter).setStepSize(step).setRegParam(regParam)
    val model = alg.run(train)
    val rescaledPredicts = train.map(x => (model.predict(x.features), x.label))
    val validPredicts = test.map(x => (model.predict(x.features), x.label))
    val meanSquared = math.sqrt(rescaledPredicts.map({case(p,l) => math.pow(p-l,2)}).mean())
    val meanSquaredValid = math.sqrt(validPredicts.map({case(p,l) => math.pow(p-l,2)}).mean())
    println("%d, %5.3f -> %.4f, %.4f".format(numIter, step, meanSquared, meanSquaredValid))
    println("\tweights: "+model.weights)
  }
}
iterateRidge(Array(200, 400, 1000, 3000, 6000, 10000), Array(1.1), 0.01, trainHPScaled, validHPScaled)
// Our results:
// 200, 1.100 -> 4.2354, 4.0095
// 400, 1.100 -> 4.1355, 3.9790
// 1000, 1.100 -> 4.0425, 3.9661
// 3000, 1.100 -> 3.9842, 3.9695
// 6000, 1.100 -> 3.9674, 3.9728
// 10000, 1.100 -> 3.9607, 3.9745
iterateLasso(Array(200, 400, 1000, 3000, 6000, 10000, 15000), Array(1.1), 0.01, trainHPScaled, validHPScaled)
//Our results:
// 200, 1.100 -> 4.1762, 4.0223
// 400, 1.100 -> 4.0632, 3.9964
// 1000, 1.100 -> 3.9496, 3.9987
// 3000, 1.100 -> 3.8636, 4.0362
// 6000, 1.100 -> 3.8239, 4.0705
// 10000, 1.100 -> 3.7985, 4.1014
// 15000, 1.100 -> 3.7806, 4.1304

```

### 7.6.6 K-fold 

8장에서 설명

## 7.7 알고리즘 성능 최적화

미니배치 SGD, LBFGS최적화를 사용. 

### 7.7.1 미니배치 기반 확률적 경사하강법 

코드는 거의 복붙인듯...

```scala
def iterateLRwSGDBatch(iterNums:Array[Int], stepSizes:Array[Double], fractions:Array[Double], train:RDD[LabeledPoint], test:RDD[LabeledPoint]) = {
  for(numIter <- iterNums; step <- stepSizes; miniBFraction <- fractions)
  {
    val alg = new LinearRegressionWithSGD()
    alg.setIntercept(true).optimizer.setNumIterations(numIter).setStepSize(step)
    alg.optimizer.setMiniBatchFraction(miniBFraction)
    val model = alg.run(train)
    val rescaledPredicts = train.map(x => (model.predict(x.features), x.label))
    val validPredicts = test.map(x => (model.predict(x.features), x.label))
    val meanSquared = math.sqrt(rescaledPredicts.map({case(p,l) => math.pow(p-l,2)}).mean())
    val meanSquaredValid = math.sqrt(validPredicts.map({case(p,l) => math.pow(p-l,2)}).mean())
    println("%d, %5.3f %5.3f -> %.4f, %.4f".format(numIter, step, miniBFraction, meanSquared, meanSquaredValid))
  }
}
iterateLRwSGDBatch(Array(400, 1000), Array(0.05, 0.09, 0.1, 0.15, 0.2, 0.3, 0.35, 0.4, 0.5, 1), Array(0.01, 0.1), trainHPScaled, validHPScaled)
//Our results:
// 400, 0.050 0.010 -> 6.0134, 5.2776
// 400, 0.050 0.100 -> 5.8968, 4.9389
// 400, 0.090 0.010 -> 4.9918, 4.5734
// 400, 0.090 0.100 -> 4.9090, 4.3149
// 400, 0.100 0.010 -> 4.9461, 4.5755
// 400, 0.100 0.100 -> 4.8615, 4.3074
// 400, 0.150 0.010 -> 4.8683, 4.6778
// 400, 0.150 0.100 -> 4.7478, 4.2706
// 400, 0.200 0.010 -> 4.8797, 4.8388
// 400, 0.200 0.100 -> 4.6737, 4.2139
// 400, 0.300 0.010 -> 5.1358, 5.3565
// 400, 0.300 0.100 -> 4.5565, 4.1133
// 400, 0.350 0.010 -> 5.3556, 5.7160
// 400, 0.350 0.100 -> 4.5129, 4.0734
// 400, 0.400 0.010 -> 5.4826, 5.8964
// 400, 0.400 0.100 -> 4.5017, 4.0547
// 400, 0.500 0.010 -> 16.2895, 17.5951
// 400, 0.500 0.100 -> 5.0948, 4.6498
// 400, 1.000 0.010 -> 332757322790.5126, 346531473220.8046
// 400, 1.000 0.100 -> 179186352.6756, 189300221.1584
// 1000, 0.050 0.010 -> 5.0089, 4.5567
// 1000, 0.050 0.100 -> 4.9747, 4.3576
// 1000, 0.090 0.010 -> 4.7871, 4.5875
// 1000, 0.090 0.100 -> 4.7493, 4.3275
// 1000, 0.100 0.010 -> 4.7629, 4.5886
// 1000, 0.100 0.100 -> 4.7219, 4.3149
// 1000, 0.150 0.010 -> 4.6630, 4.5623
// 1000, 0.150 0.100 -> 4.6051, 4.2414
// 1000, 0.200 0.010 -> 4.5948, 4.5308
// 1000, 0.200 0.100 -> 4.5104, 4.1800
// 1000, 0.300 0.010 -> 4.5632, 4.5189
// 1000, 0.300 0.100 -> 4.3682, 4.0964
// 1000, 0.350 0.010 -> 4.5500, 4.5219
// 1000, 0.350 0.100 -> 4.3176, 4.0689
// 1000, 0.400 0.010 -> 4.4396, 4.3976
// 1000, 0.400 0.100 -> 4.2904, 4.0562
// 1000, 0.500 0.010 -> 11.6097, 12.0766
// 1000, 0.500 0.100 -> 4.5170, 4.3467
// 1000, 1.000 0.010 -> 67686532719.3362, 62690702177.4123
// 1000, 1.000 0.100 -> 103237131.4750, 119664651.1957
iterateLRwSGDBatch(Array(400, 1000, 2000, 3000, 5000, 10000), Array(0.4), Array(0.1, 0.2, 0.4, 0.5, 0.6, 0.8), trainHPScaled, validHPScaled)
//Our results:
// 400, 0.400 0.100 -> 4.5017, 4.0547
// 400, 0.400 0.200 -> 4.4509, 4.0288
// 400, 0.400 0.400 -> 4.4434, 4.1154
// 400, 0.400 0.500 -> 4.4059, 4.1588
// 400, 0.400 0.600 -> 4.4106, 4.1647
// 400, 0.400 0.800 -> 4.3930, 4.1245
// 1000, 0.400 0.100 -> 4.2904, 4.0562
// 1000, 0.400 0.200 -> 4.2470, 4.0363
// 1000, 0.400 0.400 -> 4.2490, 4.0181
// 1000, 0.400 0.500 -> 4.2201, 4.0372
// 1000, 0.400 0.600 -> 4.2209, 4.0357
// 1000, 0.400 0.800 -> 4.2139, 4.0173
// 2000, 0.400 0.100 -> 4.1367, 3.9843
// 2000, 0.400 0.200 -> 4.1030, 3.9847
// 2000, 0.400 0.400 -> 4.1129, 3.9736
// 2000, 0.400 0.500 -> 4.0934, 3.9652
// 2000, 0.400 0.600 -> 4.0926, 3.9849
// 2000, 0.400 0.800 -> 4.0893, 3.9793
// 3000, 0.400 0.100 -> 4.0677, 3.9342
// 3000, 0.400 0.200 -> 4.0366, 4.0256
// 3000, 0.400 0.400 -> 4.0408, 3.9815
// 3000, 0.400 0.500 -> 4.0282, 3.9833
// 3000, 0.400 0.600 -> 4.0271, 3.9863
// 3000, 0.400 0.800 -> 4.0253, 3.9754
// 5000, 0.400 0.100 -> 3.9826, 3.9597
// 5000, 0.400 0.200 -> 3.9653, 4.0212
// 5000, 0.400 0.400 -> 3.9654, 3.9801
// 5000, 0.400 0.500 -> 3.9600, 3.9780
// 5000, 0.400 0.600 -> 3.9591, 3.9774
// 5000, 0.400 0.800 -> 3.9585, 3.9761
// 10000, 0.400 0.100 -> 3.9020, 3.9701
// 10000, 0.400 0.200 -> 3.8927, 4.0307
// 10000, 0.400 0.400 -> 3.8920, 3.9958
// 10000, 0.400 0.500 -> 3.8900, 4.0092
// 10000, 0.400 0.600 -> 3.8895, 4.0061
// 10000, 0.400 0.800 -> 3.8895, 4.0199
```

### 7.7.2 LBFGS 최적화

이것도 코드복붙..

```scala
import org.apache.log4j.Logger
import org.apache.log4j.Level
Logger.getLogger("breeze").setLevel(Level.WARN)
def iterateLBFGS(regParams:Array[Double], numCorrections:Int, tolerance:Double, train:RDD[LabeledPoint], test:RDD[LabeledPoint]) = {
  import org.apache.spark.mllib.optimization.LeastSquaresGradient
  import org.apache.spark.mllib.optimization.SquaredL2Updater
  import org.apache.spark.mllib.optimization.LBFGS
  import org.apache.spark.mllib.util.MLUtils
  val dimnum = train.first().features.size
  for(regParam <- regParams)
  {
    val (weights:Vector, loss:Array[Double]) = LBFGS.runLBFGS(
      train.map(x => (x.label, MLUtils.appendBias(x.features))),
      new LeastSquaresGradient(),
      new SquaredL2Updater(),
      numCorrections,
      tolerance,
      50000,
      regParam,
      Vectors.zeros(dimnum+1))

    val model = new LinearRegressionModel(
      Vectors.dense(weights.toArray.slice(0, weights.size - 1)),
      weights(weights.size - 1))

    val trainPredicts = train.map(x => (model.predict(x.features), x.label))
    val validPredicts = test.map(x => (model.predict(x.features), x.label))
    val meanSquared = math.sqrt(trainPredicts.map({case(p,l) => math.pow(p-l,2)}).mean())
    val meanSquaredValid = math.sqrt(validPredicts.map({case(p,l) => math.pow(p-l,2)}).mean())
    println("%5.3f, %d -> %.4f, %.4f".format(regParam, numCorrections, meanSquared, meanSquaredValid))
  }
}
iterateLBFGS(Array(0.005, 0.007, 0.01, 0.02, 0.03, 0.05, 0.1), 10, 1e-5, trainHPScaled, validHPScaled)
//Our results:
// 0.005, 10 -> 3.8335, 4.0383
// 0.007, 10 -> 3.8848, 4.0005
// 0.010, 10 -> 3.9542, 3.9798
// 0.020, 10 -> 4.1388, 3.9662
// 0.030, 10 -> 4.2892, 3.9996
// 0.050, 10 -> 4.5319, 4.0796
// 0.100, 10 -> 5.0571, 4.3579

```

## 7.8 요약

- 지도 학습 알고리즘은 레이블 데이터를 학습에 사용한다. 반면 비지도 학습 알고리즘은 모델 학습으로 레이블이 없는 데이터 내부 구조를 발굴한다. 
- 지도학습 알고리즘은 목표 변수의 유형에 따라 다시 회귀와 분류로 나눈다. 회귀는 연속적인 값(실수)를 목표 변수로 사용하며, 분류는 범주(이산 값의 집합)을 사용한다. 
- 데이터를 선형 회귀에 활용하기 앞서 데이터 분포와 특징 변수간의 유사도를 분석하는 것이 좋다. 또 데이터에 정규화 및 스케일링 기법을 적용하고, 전체 데이터셋을 훈련 데이터셋과 검증데이터셋으로 분할해야 한다. 
- 평균 제곱근 오차(RMSE)는 선형 회귀 모델의 성능을 평가하는 보편적인 지표이다.
- 모델이 학습한 각 가중치 값은 개별 특징 변수가 목표 변수에 미치는 영향력을 의미한다.
- 데이터셋에 고차 다항식을 추가해 비선형 문제에 선형 회귀를 적용할 수 있으며, 일부 데이터셋에서는 더 나은 성능 결과를 얻을 수 있다. 
- 모델의 복잡도를 증가시키면 과적합이 발생할 수 있다. 편향-분산 상충 관계는 모델이 높은 편향이나 높은 분산을 가지도록 만들 수는 있지만, 이둘을 동시에 높일 수 없다는 것을 의미한다. 
- 릿지 회귀와 라소 회귀는 선형 회귀의 과적합을 방지할 수 있다. 
- 미니배치 기반 확률적 경사 하강법은 선형 회귀 알고리즘의 계산 성능을 최적화한다.
- 스파크의 LBFGS 최적화는 모델 훈련 시간을 크게 단축시키고 계산 성능을 상당히 개선할 수 있다. 

