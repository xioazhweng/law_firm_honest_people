my_project/
│
├── README.md                # Описание проекта
├── db/
│   ├── create_db.sql        # SQL для создания базы и схемы (tables, constraints)
│   ├── drop_db.sql          # SQL для удаления/очистки базы
│   └── init_db.py           # Python-скрипт для автоматизации создания базы через psycopg2
│
├── data_generation/
│   ├── generate_data.py     # Скрипт на Python с Faker для заполнения таблиц
│   └── sample_data.json     # (опционально) тестовые данные или шаблоны
│
├── queries/
│   ├── select_queries.sql   # SQL-запросы на выборку
│   ├── update_queries.sql   # SQL-запросы на обновлениев
│
└── scripts/
    ├── reset_db.sh          # Скрипт bash для автоматического пересоздания базы
    
