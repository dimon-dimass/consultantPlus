# ConsultantPlus Test Case

## Структура проекта

- **src/main/scala/EventCount.scala**: Исходный код объекта, решающего первую задачу.
- **src/main/scala/QSDocOpenCount.scala**: Исходный код объекта, решающего вторую задачу.
- **pom.xml**: Файл формата (`.xml`), предназначенный для сборки зависимостей с помощью архитектуры Apache Maven.
- **output/task1**: Результаты выполнения задания №1 сохраняются в текстовом формате.
- **output/task2**: Результаты выполнения задания №2 сохраняются в текстовом формате.

## Установка необходимых компонентов (Windows)

- **Java SDK** - Скачать и установить набор инструментов Java (версии 8, 11 или 17) по ссылке: https://www.oracle.com/java/technologies/downloads/. Установить в переменных окружения *JAVA_HOME = :path/to/java*.
- **Apache Spark** - Скачать фреймворк по ссылке: *https://spark.apache.org/downloads.html*. Установить в переменных окружения *SPARK_HOME = :path/to/spark* и *Path = SPARK_HOME/bin*
- **Apache Maven** - Скачать фреймворк по ссылке: *https://maven.apache.org/download.cgi*. Установить в переменной окружения *Path = :path/to/maven/bin*
- **Scala** - Скачать установщик Coursier по ссылке: *https://www.scala-lang.org/download/*. Установить в переменной окружения *Path = :path/to/Coursier/bin*
- winutil.exe for Hadoop - Скачать необходимые компоненты для корректной работы с SparkContext по ссылке: *https://github.com/cdarlint/winutils/tree/master*. Установить в переменных окружения *HADOOP_HOME = :path/to/hadoop* и *Path = HADOOP_HOME/bin*. В случае, возникновения повторяющейся ошибки:
  `Exception in thread "main" java.lang.UnsatisfiedLinkError: 'boolean org.apache.hadoop.io.nativeio.NativeIO$Windows.access0(java.lang.String, int)'`
поместить hadoop.dll в *C:Windows/System32*.

## EvenCount.scala (Задание №1)

Чтобы запустить программу необходимо выполнить предварительную настройку проекта [Запуск проекта через Intellij IDEA](#Запуск-проекта) и затем в конфигурации установить необходимые аргументы в соответствующем поле:

```scala
EventCount arg1 arg2 arg3
-arg1      file- or dirpath
-arg2      one of Events [QS, DOC_OPEN, CARD_SEARCH_START]
-arg3      document id | array of doc ids | filepath to doc id
```
Результатом работы программы будет являться текстовый файл содержащий три поля, разделенные запятыми: 
- **Имя_События**;
- **Идентификатор_Документа**;
- **Количество**.

## Задание №2

Чтобы запустить программу необходимо выполнить предварительную настройку проекта [Запуск проекта через Intellij IDEA](#Запуск-проекта) и затем в конфигурации установить необходимые аргументы в соответствующем поле:
```scala
QSDocOpenCount arg1
-arg1      file- or dirpath
```
Результирующий формат записи для задания №2 представляет собой последовательность, разделенную запятыми, из следующих данных:
- **Дата**;
- **Идентификатор_События**;
- **Идентификатор_Документа**;
- **Количество_открытий**: Количество открытий документа в разрезе дня.


## Запуск проекта через IntelliJ IDEA

Для корректного запуска проекта выполните следующие шаги:

1. **Клонирование репозитория**:
   - Склонируйте данный репозиторий на ваш компьютер.

2. **Подготовка данных**:
   - Поместите директорию с файлами данных в корневую директорию проекта под именем `sessions`.

3. **Настройка конфигурации запуска**:
   - Перейдите в меню `Run -> Edit Configurations`.
   - Добавьте новую конфигурацию (`Add New Configuration -> Application`).
   - Укажите имя конфигурации.
   - Выберите версию Java SDK (8, 11 или 17).
   - В разделе `Build and Run` установите опцию через `Modify options`: `Use classpath of module`.
   - Укажите classpath и модуль.
   - Нажмите `Apply` и `Run`.
