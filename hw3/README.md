## Hw 3

Запуск через docker compose up 

Для проверки останавливаю результат и дожидаюсь обрабокти оставшихся событий

```sh
docker compose stop producer-1 producer-2 producer-3
./verify --wait 120
```


Эксперименты запускаются скриптом
```
python3 scripts/experiments.py all --reset
```



Вместо all можно указать kill, brokers или ordering

## Гипотезы

### Падение консьюмера
