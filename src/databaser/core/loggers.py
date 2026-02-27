from collections import (
    defaultdict,
)
from contextlib import (
    asynccontextmanager,
)
from datetime import (
    datetime,
)
from typing import (
    Iterable,
)

import psutil

from databaser.core.db_entities import (
    DBTable,
    DstDatabase,
)
from databaser.core.enums import (
    StagesEnum,
)
from databaser.core.helpers import (
    dates_to_string,
    logger,
)


class StatisticManager:
    """Менеджер для сбора и анализа статистики процесса переноса данных.

    Класс отвечает за:
    - Сбор временных показателей каждого этапа
    - Мониторинг использования оперативной памяти
    - Подсчет количества перенесенных записей
    - Формирование отчетов о процессе переноса
    """

    def __init__(
        self,
        database: DstDatabase,
    ):
        """Инициализация менеджера статистики.

        Args:
            database: Целевая база данных, для которой собирается статистика
        """
        self._database = database

        self._time_indications = defaultdict(list)
        self._memory_usage_indications = defaultdict(list)

    def set_indication_time(self, stage):
        """Фиксация времени этапа.

        Добавляет текущее время в список временных меток для указанного этапа.
        Каждый этап имеет две метки: начало и конец выполнения.

        Args:
            stage: Номер этапа из StagesEnum
        """
        self._time_indications[stage].append(datetime.now())

    def set_indication_memory(self, stage):
        """Фиксация используемой оперативной памяти на этапе.

        Сохраняет информацию об использовании памяти в момент начала
        и завершения каждого этапа.

        Args:
            stage: Номер этапа из StagesEnum
        """
        self._memory_usage_indications[stage].append(dict(psutil.virtual_memory()._asdict()))

    def print_stages_indications(self):
        """Печать показателей всех этапов работы.

        Выводит в лог для каждого этапа:
        - Название этапа
        - Временные метки начала и окончания
        - Показатели использования памяти
        """
        for stage in StagesEnum.values.keys():
            if stage in self._time_indications:
                logger.info(f'{StagesEnum.values.get(stage)} --- {dates_to_string(self._time_indications[stage])}')

            if stage in self._memory_usage_indications:
                logger.info(f'{StagesEnum.values.get(stage)} --- {self._memory_usage_indications[stage]}')

    def print_records_transfer_statistic(self):
        """Печать статистики перенесенных записей в целевую базу данных.

        Для каждой таблицы выводит:
        - Название таблицы
        - Количество успешно перенесенных записей
        - Общее количество записей, которые требовалось перенести

        Таблицы сортируются по количеству перенесенных записей.
        """
        tables: Iterable[DBTable] = self._database.tables.values()
        tables_counts = {table.name: (table.transferred_pks_count, len(table.need_transfer_pks)) for table in tables}

        sorted_tables_counts = sorted(tables_counts, key=lambda t_n: tables_counts[t_n][0])

        for table_name in sorted_tables_counts:
            logger.info(f'{table_name} --- {tables_counts[table_name][0]} / {tables_counts[table_name][1]}')


@asynccontextmanager
async def statistic_indexer(
    statistic_manager: StatisticManager,
    stage: int,
):
    """Асинхронный контекстный менеджер для сбора статистики этапа работы.

    При входе в контекст фиксирует начальное время и использование памяти.
    При выходе из контекста фиксирует конечное время и использование памяти.

    Args:
        statistic_manager: Менеджер статистики для сбора данных
        stage: Номер этапа из StagesEnum

    Yields:
        None
    """
    statistic_manager.set_indication_time(stage)
    statistic_manager.set_indication_memory(stage)

    yield

    statistic_manager.set_indication_time(stage)
    statistic_manager.set_indication_memory(stage)
