from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Функция для вывода треугольника Паскаля
def print_pascals_triangle():
    n = 10  # 10 уровней
    triangle = [[1]]

    for i in range(1, n):
        row = [1]
        for j in range(1, i):
            row.append(triangle[i-1][j-1] + triangle[i-1][j])
        row.append(1)
        triangle.append(row)

    # Вывод треугольника Паскаля
    for row in triangle:
        print(" ".join(map(str, row)))

# Определение DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
}

dag = DAG(
    'pascal',
    default_args=default_args,
    description='DAG для вывода треугольника Паскаля',
    schedule_interval='44 11 * * *',  # Ежедневный запуск в 11:44
    start_date=datetime(2024, 12, 22),  # Дата начала
    catchup=False,
)

# Оператор для запуска Python функции
task = PythonOperator(
    task_id='print_pascals_triangle_task',
    python_callable=print_pascals_triangle,
    dag=dag,
)

task
