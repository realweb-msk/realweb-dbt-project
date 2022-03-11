# dbt проект Реалвеб

## Почему dbt?

* Контроль версии SQL моделей
* Написание тестов на схемы данных и на качество данных
* Отправка алёртов по результам тестов
* Автоматическая документация
* Оптимизация SQL за счёт переиспользования моделей
* Более гибкий аналог Scheduled Queries в BigQuery
* [и многое другое](https://docs.getdbt.com/docs/introduction)

## Публичная документация

*пока нет*

## Как начать работать с проектом?

Спасибо [@nirakon](https://github.com/nirakon) за подробную инструкцию.

1. [Устанавливаем Miniconda с Python 3.8](https://docs.conda.io/en/latest/miniconda.html)
2. [Устанавливаем Visual Studio Code](https://code.visualstudio.com/download)
3. [Устанавливаем Git](https://git-scm.com/download)
4. Запускаем Anaconda Prompt (Miniconda3). Создаём новое пространство для dbt (версия python = 3.8.5) `conda create --name dbt-env python=3.8.5 pip`
5. Переходим в него `conda activate dbt-env` и устанавливаем dbt `pip install dbt-bigquery`
6. Проверяем установку `dbt --version`
7. Устанавливаем расширение `ms-python.python` в VSCode.
8. В VSCode назначаем интерпетатор для Python в созданном пространства dbt-env
9. Подключаемся к GitHub в VSCode и [скачиваем нужный репозиторий](https://code.visualstudio.com/docs/editor/versioncontrol#_cloning-a-repository) (https://github.com/realweb-msk/realweb-dbt)
10. Настраиваем `profiles.yml` для подключения к BigQuery. Этот файл обычно находится за пределами вашего проекта dbt, чтобы избежать передачи конфиденциальных учетных данных в git. По умолчанию dbt предполагает, что файл `profiles.yml` будет расположен в каталоге `~/.dbt/`. [Подробнее](https://docs.getdbt.com/reference/warehouse-profiles/bigquery-profile/#service-account-file).
Также необходимо получить json-ключ в GCP *(или попросить его у меня)* и положить его в надёжное место (например,в папку `secrets`)

 ```yml
# Пример profiles.yml. Обычно используется две среды dev (development) и prod (production)

my-bigquery-db: # Название профиля, которое будет указано в dbt_project.yml в profile. В данном случае это "realweb"
  target: dev 
  outputs:
    dev:
      type: bigquery
      method: service-account # Способ авторизации с помощью json ключа
      project: [GCP project id] # Название проекта в BQ, в нашем случае это realweb-152714
      dataset: [the name of your dbt dataset] # Обычно dbt_username (dbt_rsultanov)
      threads: [1 or more] # Для локальной разработки можно поставить "4"
      keyfile: [/secrets/your_keyfile] # путь к json ключу за пределами проекта dbt
      timeout_seconds: 300 
      location: US # Лучше не указывать, тогда будет локация по умолчанию, которая стоит в проекте BQ
      priority: interactive
      retries: 1

     prod:
       type: bigquery
       method: service-account 
       project: [GCP project id] # Название проекта в BQ
       dataset: dbt_production
       threads: 1
       keyfile: [/secrets/your_keyfile]
       timeout_seconds: 300
       priority: interactive
       retries: 1
 ```

11. Выполняем в консоли `dbt debug`. Если всё хорошо, можно начать пользоваться dbt.

## Если я хочу создать свой проект?

1. [Укрепиться в решении - Вводный вебинар от OWOX про dbt](https://www.youtube.com/watch?v=eLDV_y0Chow)
2. [Пройти небольшой бесплатный курс по dbt](https://courses.getdbt.com/)
3. [Первые шаги](https://docs.getdbt.com/dbt-cli/install/overview)
4. [Запускаем dbt в продакшн на Google Cloud Platform](https://github.com/realweb-msk/realweb-dbt)

## Задание по dbt на пару вечеров

1. Пройдите указанным выше путем (**Как начать работать с проектом**). Если не будет хватать каких-то разрешений - выдадим.
2. Выполните в консоли `dbt run` - так все имеющиеся в проекте модели попадут в ваш датасет **dbt_username**
3. Загляните в файл `src_staging.yml`, лежащий в папке staging, добавьте в него какой-нибудь другой источник данных из того же датасета **hackaton** или из другого. Location у датасета должна быть в US.
4. В папке **staging** создайте свою первую модель на основании добавленного вами источника. В модели должна быть минимальная предобработка - измените названия столбцов,которые вам не нравятся, измените форматы данных... В консоли выполните  `dbt run -m your_model_name` для того,чтобы модель попала в ваш именной датасет, и   `dbt run -m your_model_name -t prod`, чтобы она оказалась в датасете **dbt_production**. Если при выполнении команд обнаружатся ошибки в SQL, просто исправьте их:) Пример stg-модели - **stg_app_installs**, пример команды для ее материализации в dbt_production: `dbt run -m stg_app_installs -t prod`
5. В папке **marts** создайте еще одну модель,которая будет обращаться к вашей stg-модели. Это может быть что угодно - агрегация по дням, источникам, кастомная атрибуция... Что угодно, что вы сочтете полезным. Пример - **app_installs_by_city**. Выполните команду `dbt run` для вашей модели, например `dbt run -m app_installs_by_city`, для dev и prod.
6. В файле `stg_schema.yml` создайте заготовку документации для вашей stg-модели (если вы тщательно опишете модель и все столбцы, будет просто бомба). Для какого-нибудь столбца пропишите [стандартный тест](https://docs.getdbt.com/docs/building-a-dbt-project/tests#generic-tests) (в примере это accepted_values) или [напишите свой](https://docs.getdbt.com/docs/building-a-dbt-project/tests#getting-started) (это задание со звёздочкой). Выполните `dbt test` для вашей модели, например `dbt test -m stg_app_installs`

