# Юридическая фирма "Честные люди"

Реализация в соответсвии с техническим заданием базы данных юридической фирмы.

### Build database
```bash
./scripts/reset_db.sh 
```

### Структура проекты
```

   src
    ├── acid
    │   ├── main.py
    │   └── test.sh
    ├── create_db
    │   ├── create_db.sql
    │   └── reset_db.sh
    ├── data_generation
    │   ├── DBFiller.py
    │   ├── GenLib.py       
    │   └── main.py
    ├── indexes
    │   ├── 1.sql
    │   └── 3.sql
    ├── procedures
    │   ├── 1.sql
    │   ├── 2.sql
    │   ├── test.py
    │   └── test.sh
    ├── queries
    │   ├── 1.sql
    │   ├── 2.sql
    │   ├── 3.sql
    │   ├── 4.sql
    │   └── 5.sql
    └── triggers
        ├── 1.sql
        ├── 2.sql
        ├── test.log
        ├── test.py
        └── test.sh
```
