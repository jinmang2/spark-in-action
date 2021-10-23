# Spark in Action
## chapter 2

https://github.com/spark-in-action/first-edition  깃헙 레포

개발환경세팅 pass

-----------------

## RDD(Resilient Distributed Dataset)의 개념

1장의 RDD의 내용

RDD는 분산 데이터셋을 추상화한 객체 -> 데이터셋에 적용할 수 있는 연산 및 변환 메서드를 함께 제공한다. 

RDD는 노드에 장애가 발생해도 데이터셋을 재구성할 수 있는 복원성을 갖추었다. 

2장내용

licLines, bsdLines는 RDD라는 스파크 전용 분산 컬렉션이다.

RDD는 스파크의 기본 추상화 객체로 다음의 성질을 가지고 있다.

- 불변성(immutable): 읽기전용

    RDD는 데이터를 조작할 수 있는 다양한 변환 연산자를 제공하지만 변환 연산자는 항상 새로운 RDD 객체를 생성한다. -> 한번 생성된 RDD는 절때 바뀌지 않는 불변의 성질.

    불변의 성질을 이용해 장애 내성을 직관적인 방법으로 보장. 

- 복원성(resilient): 장애 내성

    다른 분산 프레임 워크들은 장애가 발생하면 데이터의 복제본을 가져와서 데이터를 복구

    하지만 RDD는 데이터셋 자체를 중복 저장하지 않는 대신 데이터셋을 만드는 데 사용된 로그 를 남기는 방식으로 장애 내성을 가짐. -> 일부 노드에 장애가 발생하면 스파크는 해당 노드가 가진 데이터셋만 다시 계산해 RDD를 복원함.

- 분산(distributed): 노드 한 개 이상에 저장된 데이터셋

    RDD는 투명성을 가지고 있다. 

    투명성이란 파일의 물리적인 섹터를 여러 장소에 나누어 저장해도 파일 이름만으로 파일에 접근할 수 있다는것. 

RDD의 목적은 분산 컬렉션의 성질과 장애 내성을 추상화 하고 직관적인 방식으로 대규모 데이터셋에 병렬 연산을 수행할 수 있도록 지원하는 것이다. 


## RDD의 기본 행동 연산자 및 변환 연산자

RDD 연산자는 크게 두가지로 나눔

- 변환 연산자 
    
    RDD의 데이터를 조작해 새로운 RDD를 생성한다. (ex: filter, map)

- 행동연산자 

    연산자를 호출한 프로그램으로 계산결과를 반환하거나 RDD 요소에 특정 작업을 수행하려고 실제 계산을 시작하는 역할을 한다. (count, foreach)

### **filter**

filter는 liclines 컬렉션의 각 요소를 굵은 화살표로 정의한 익명함수에 전달하고 익명함수가 Ture로 판변한 요소만으로 구성된 새로운 컬렉션 bsdlines를 반환
(주어진 조건에 따라 RDD의 일부 요소를 제거)

![image](https://user-images.githubusercontent.com/60643542/134660859-a7394267-c988-4e1c-abb0-c3b6a39c353c.png)

![image](https://user-images.githubusercontent.com/60643542/134660948-fa84d1bc-f72c-45b1-8eab-a7ad5b6382de.png)


### **map**

원본 RDD의 각 요소를 변환후 변환된 요소로 새로운 RDD를 생성

(map은 RDD의 모든 요소에 임의의 함수를 적용할 수 있는 변환 연산자)


![image](https://user-images.githubusercontent.com/60643542/134662148-9024c835-8c5e-4027-91d4-02563f7f6e20.png)


### **distinct, flatMap**

collect는 행동 연산자 -> 새로운 배열을 생성하고 RDD의 모든 요소를 이 배열에 모아 스파크 셸로 반환

flatMap은 collect해서 모은 여러 배열의 요소를 하나의 단일 배열로 통합할 수 있다. 

distinct는 중복요소를 제거한 새로운 RDD를 반환

![image](https://user-images.githubusercontent.com/60643542/134666682-96be2233-c917-4f14-80c9-fea75ac59145.png)

### **sample, take, takeSample**

sample은 호출된 RDD에서 무작위로 요소를 뽑아 새로운 RDD를 만드는 변환 연산자.(0.3)같은 확률값으로 연산

takeSample은 sample처럼 확률값 연산이 아닌 int를 input으로 받아 int의 값으로 갯수를 반환, 그리고 배열을 반환하는 행동 연산자이다. 

take는 데이터의 하위 집합을 가져올 수 있는 행동 연산자이다. 지정된 개수의 요소를 모을때까지 RDD의 파티션(클러스터의 여러 노드에 저장된 데이터의 일부분)을 하나씩 처리해 결과를 반환한다. 이 연산자는 RDD의 데이터를 살짝 엿볼때 자주 사용한다고 함.

![image](https://user-images.githubusercontent.com/60643542/134667888-8ae46094-dec3-4a26-9a61-13bea4c37ecf.png)


## Double RDD 전용 함수 

double 객체만 사용해 RDD를 구성하면 암시적 변환(implicit conversion)을 이용해 몇가지 함수를 추가해서 사용할 수 있다. 

### **암시적 변환이란?**

classone 클래스는 타입 매개변수를 가지므로 여러 타입을 input으로 받을 수 있다. 
이 classone클래스를 int, string을 input으로 받는 각각의 클래스로 암시적으로 변환해서 사용할 수 있다. 

![image](https://user-images.githubusercontent.com/60643542/134669683-140e77c6-0f56-4a60-b774-9a9d7e8fabe9.png)


double RDD는 요소의 전체 합계, 평균, 표준편차, 분산, 히스토그램을 계산하는 함수들을 제공한다. -> 데이터 분포 파악하는데 유용

![image](https://user-images.githubusercontent.com/60643542/134670796-10d488d6-d320-4fc2-8a52-7a070b55b5a4.png)

sumApprox 근사 합계, meanApprox 근사 평균은 제한된 시간을 인자로 받아 시간내에 결과가 안나오면 중간 결과값을 반환한다. 

## 2장 요약
- 하둡과 마찬가지로 심볼릭 링크로 여러 스파크 릴리스를 관리할 수 있다. 
- 스파크 셸은 일회성 작업이나 EDA에 주로 사용한다. 
- RDD는 스파크의 기본 추상화 객체이다. RDD는 불변성과 복원성을 가진 분산 컬렉션이다.
- RDD의 연산자 유형에는 변환 연산자와 행동 연산자가 있다. 
    - 변환 연산자는 RDD의 데이터를 조작해 새로운 RDD를 생성한다(ex: filter, map) 
    - 행동 연산자는 연산자를 호출한 프로그램으로 계산결과를 반환하거나 RDD요소에 특정 작업을 수행하려고 실제 계산을 시작하는 역할을 한다(ex: count, foreach)
- map은 RDD데이터를 변환하는 데 사용하는 주요 연산자이다.
- distinct는 고유한 RDD요소로 구성된 새로운 RDD를 반환한다.
- flatMap은 익명 함수가 반환한 배열의 중첩 구조를 한단계 제거하고 모든 배열의 요소를 단일 컬렉션으로 병합한다.
- sample. take. takeSample 연산자를 사용해 RDD의 요소를 일부 가져올 수 있다. 
- 스칼라의 암시적 변환으로 double RDD 함수를 사용할 수 있으며 RDD 요소의 전체 합계, 평균값, 표준편차, 분산, 히스토그램을 계산할 수 있다. 