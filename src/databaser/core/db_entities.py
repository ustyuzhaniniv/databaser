import asyncio
import traceback
from collections import (
    defaultdict,
)
from functools import (
    lru_cache,
)
from typing import (
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    Union,
)

import asyncpg
from asyncpg.exceptions import (
    InvalidNameError,
)
from asyncpg.pool import (
    Pool,
)

from databaser.core.enums import (
    ConstraintTypesEnum,
)
from databaser.core.helpers import (
    DBConnectionParameters,
    deep_getattr,
    logger,
    make_chunks,
    make_str_from_iterable,
)
from databaser.core.repositories import (
    SQLRepository,
)
from databaser.core.strings import (
    CONNECTION_STR_TEMPLATE,
)
from databaser.settings import (
    EXCLUDED_TABLES,
    IS_TRUNCATE_TABLES,
    KEY_COLUMN_NAMES,
    KEY_TABLE_NAME,
    TABLES_LIMIT_PER_TRANSACTION,
    TABLES_TRUNCATE_EXCLUDED,
    TABLES_TRUNCATE_INCLUDED,
    TABLES_WITH_GENERIC_FOREIGN_KEY,
)


class BaseDatabase(object):
    """Базовый класс для работы с базами данных.

    Предоставляет основные функции для:
    - Подключения к базе данных
    - Управления пулом соединений
    - Получения списка таблиц и секций
    - Выполнения SQL-запросов
    """

    def __init__(
        self,
        db_connection_parameters: DBConnectionParameters,
    ):
        """Инициализация базы данных.

        Args:
            db_connection_parameters: Параметры подключения к базе данных
        """
        self.db_connection_parameters: DBConnectionParameters = db_connection_parameters
        self.table_names: Optional[List[str]] = None
        self.partition_names: Optional[List[str]] = None
        self.tables: Optional[Dict[str, DBTable]] = None

        self._connection_pool: Optional[Pool] = None

    @property
    def connection_str(self) -> str:
        """Получение строки подключения к базе данных.

        Returns:
            str: Отформатированная строка подключения
        """
        return CONNECTION_STR_TEMPLATE.format(self.db_connection_parameters)

    @property
    def connection_pool(self) -> Pool:
        """Получение пула соединений с базой данных.

        Returns:
            Pool: Активный пул соединений
        """
        return self._connection_pool

    @connection_pool.setter
    def connection_pool(
        self,
        pool: Pool,
    ):
        """Установка пула соединений с базой данных.

        Args:
            pool: Пул соединений для установки
        """
        self._connection_pool = pool

    async def prepare_partition_names(self):
        """Подготовка списка имен секций для исключения их из процесса переноса данных.

        Получает список всех секционированных таблиц в базе данных и сохраняет
        их имена в self.partition_names.
        """
        select_partition_names_list_sql = SQLRepository.get_select_partition_names_list_sql()

        async with self._connection_pool.acquire() as connection:
            partition_names = await connection.fetch(
                query=select_partition_names_list_sql,
            )

            self.partition_names = [partition_name_rec[0] for partition_name_rec in partition_names]

    async def prepare_table_names(self):
        """Подготовка списка имен таблиц базы данных.

        Получает список всех таблиц в базе данных, исключая:
        - Таблицы из списка EXCLUDED_TABLES
        - Таблицы, начинающиеся с подчеркивания
        """
        select_tables_names_list_sql = SQLRepository.get_select_tables_names_list_sql(
            excluded_tables=EXCLUDED_TABLES,
        )

        async with self._connection_pool.acquire() as connection:
            table_names = await connection.fetch(
                query=select_tables_names_list_sql,
            )

            self.table_names = [table_name_rec[0] for table_name_rec in table_names]

    async def execute_raw_sql(
        self,
        raw_sql: str,
    ):
        """Асинхронное выполнение произвольного SQL-запроса.

        Args:
            raw_sql: SQL-запрос для выполнения

        Returns:
            None

        Raises:
            asyncpg.PostgresError: При ошибке выполнения запроса
        """
        connection = await asyncpg.connect(self.connection_str)

        try:
            await connection.execute(raw_sql)
        finally:
            del raw_sql
            await connection.close()

    async def fetch_raw_sql(
        self,
        raw_sql: str,
    ):
        """Асинхронное выполнение произвольного SQL-запроса с получением результата.

        Args:
            raw_sql: SQL-запрос для выполнения

        Returns:
            List[Record]: Список записей с результатами запроса

        Raises:
            asyncpg.PostgresError: При ошибке выполнения запроса
        """
        connection = await asyncpg.connect(self.connection_str)

        try:
            result = await connection.fetch(raw_sql)
        finally:
            await connection.close()

        del raw_sql

        return result

    def clear_cache(self):
        """Очистка LRU-кэша для всех кэшируемых свойств таблиц.

        Сбрасывает кэш для следующих свойств:
        - foreign_keys_columns
        - self_fk_columns
        - not_self_fk_columns
        - fk_columns_with_key_column
        - unique_fk_columns_with_key_column
        - fk_columns_tables_with_fk_columns_with_key_column
        - unique_fk_columns_tables_with_fk_columns_with_key_column
        - highest_priority_fk_columns
        """
        DBTable.foreign_keys_columns.fget.cache_clear()
        DBTable.self_fk_columns.fget.cache_clear()
        DBTable.not_self_fk_columns.fget.cache_clear()
        DBTable.fk_columns_with_key_column.fget.cache_clear()
        DBTable.unique_fk_columns_with_key_column.fget.cache_clear()
        DBTable.fk_columns_tables_with_fk_columns_with_key_column.fget.cache_clear()
        DBTable.unique_fk_columns_tables_with_fk_columns_with_key_column.fget.cache_clear()
        DBTable.highest_priority_fk_columns.fget.cache_clear()


class SrcDatabase(BaseDatabase):
    """Класс для работы с исходной базой данных.

    Наследует функциональность BaseDatabase и добавляет специфичные
    методы для работы с базой-источником данных.
    """

    def __init__(
        self,
        db_connection_parameters: DBConnectionParameters,
    ):
        """Инициализация исходной базы данных.

        Args:
            db_connection_parameters: Параметры подключения к базе данных
        """
        logger.info('init src database')

        super().__init__(
            db_connection_parameters=db_connection_parameters,
        )


class DstDatabase(BaseDatabase):
    """Класс для работы с целевой базой данных.

    Наследует функциональность BaseDatabase и добавляет специфичные
    методы для работы с базой-приемником данных, включая:
    - Подготовку структуры таблиц
    - Очистку таблиц
    - Управление триггерами
    - Обновление последовательностей
    """

    def __init__(
        self,
        db_connection_parameters: DBConnectionParameters,
    ):
        """Инициализация целевой базы данных.

        Args:
            db_connection_parameters: Параметры подключения к базе данных
        """
        super().__init__(
            db_connection_parameters=db_connection_parameters,
        )

        logger.info('init dst database')

    @property
    @lru_cache()
    def tables_without_generics(self) -> List['DBTable']:
        """Получение списка таблиц без обобщенных внешних ключей.

        Returns:
            List[DBTable]: Список таблиц, исключая таблицы из TABLES_WITH_GENERIC_FOREIGN_KEY
        """
        return list(
            filter(
                lambda t: (t.name not in TABLES_WITH_GENERIC_FOREIGN_KEY),
                self.tables.values(),
            )
        )

    @property
    @lru_cache()
    def tables_with_key_column(self) -> List['DBTable']:
        """Получение списка таблиц с ключевой колонкой.

        Returns:
            List[DBTable]: Список таблиц, содержащих ключевую колонку
        """
        return list(
            filter(
                lambda t: t.with_key_column,
                self.tables_without_generics,
            )
        )

    async def _prepare_chunk_tables(
        self,
        chunk_table_names: Iterable[str],
    ):
        """Подготовка группы таблиц.

        Создает объекты таблиц и их колонок на основе метаданных из базы данных.

        Args:
            chunk_table_names: Список имен таблиц для подготовки
        """
        getting_tables_columns_sql = SQLRepository.get_table_columns_sql(
            table_names=make_str_from_iterable(
                iterable=chunk_table_names,
                with_quotes=True,
                quote="'",
            ),
        )

        async with self._connection_pool.acquire() as connection:
            records = await connection.fetch(
                query=getting_tables_columns_sql,
            )

        coroutines = [
            self.tables[table_name].append_column(
                column_name=column_name,
                data_type=data_type,
                ordinal_position=ordinal_position,
                constraint_table=self.tables.get(constraint_table_name),
                constraint_type=constraint_type,
            )
            for (
                table_name,
                column_name,
                data_type,
                ordinal_position,
                constraint_table_name,
                constraint_type,
            ) in records
            if constraint_table_name not in EXCLUDED_TABLES
        ]

        if coroutines:
            await asyncio.gather(*coroutines)

        self.clear_cache()

    async def prepare_tables(self):
        """Подготовка всех таблиц базы данных.

        Разбивает список таблиц на группы и асинхронно подготавливает каждую группу.
        Инициализирует словарь self.tables, где ключи - имена таблиц, значения - объекты DBTable.
        """
        logger.info('prepare tables structure for transferring process')

        self.tables = {
            f'{table_name}': DBTable(
                name=table_name,
            )
            for table_name in self.table_names
        }

        chunks_table_names = make_chunks(
            iterable=self.table_names,
            size=TABLES_LIMIT_PER_TRANSACTION,
            is_list=True,
        )

        coroutines = [
            self._prepare_chunk_tables(
                chunk_table_names=chunk_table_names,
            )
            for chunk_table_names in chunks_table_names
        ]

        if coroutines:
            await asyncio.gather(*coroutines)

        logger.info(f'prepare tables progress - {len(self.tables.keys())}/{len(self.table_names)}')

    async def set_max_tables_sequences(self):
        """Установка максимальных значений последовательностей для всех таблиц.

        Для каждой таблицы с автоинкрементным первичным ключом устанавливает
        значение последовательности равным максимальному значению в таблице + 1.
        """
        coroutines = [
            asyncio.create_task(table.set_max_sequence(self._connection_pool)) for table in self.tables.values()
        ]

        if coroutines:
            await asyncio.wait(coroutines)

    async def prepare_structure(self):
        """Подготовка структуры базы данных.

        Последовательно выполняет:
        1. Подготовку списка имен таблиц
        2. Подготовку объектов таблиц и их колонок
        """
        await self.prepare_table_names()

        await self.prepare_tables()

        logger.info(f'dst database tables count - {len(self.table_names)}')

    async def truncate_tables(self):
        """Очистка таблиц базы данных.

        Если IS_TRUNCATE_TABLES=True, очищает все таблицы, учитывая:
        - Список исключенных таблиц (TABLES_TRUNCATE_EXCLUDED)
        - Список включенных таблиц (TABLES_TRUNCATE_INCLUDED)
        """
        if IS_TRUNCATE_TABLES:
            logger.info('start truncating tables..')

            if TABLES_TRUNCATE_INCLUDED:
                table_names = TABLES_TRUNCATE_INCLUDED
            else:
                table_names = tuple(
                    filter(
                        lambda table_name: (table_name not in TABLES_WITH_GENERIC_FOREIGN_KEY),
                        self.table_names,
                    )
                )

            if TABLES_TRUNCATE_EXCLUDED:
                table_names = tuple(
                    filter(
                        lambda table_name: (table_name not in TABLES_TRUNCATE_EXCLUDED),
                        table_names,
                    )
                )

            truncate_table_queries = SQLRepository.get_truncate_table_queries(
                table_names=table_names,
            )

            for query in truncate_table_queries:
                await self.execute_raw_sql(query)

            logger.info('truncating tables finished.')

    async def disable_triggers(self):
        """Отключение всех триггеров в базе данных.

        Устанавливает состояние всех триггеров в 'DISABLED'.
        """
        disable_triggers_sql = SQLRepository.get_disable_triggers_sql()

        await self.execute_raw_sql(disable_triggers_sql)

        logger.info('trigger disabled.')

    async def enable_triggers(self):
        """Включение всех триггеров в базе данных.

        Устанавливает состояние всех триггеров в 'ORIGIN'.
        """
        enable_triggers_sql = SQLRepository.get_enable_triggers_sql()

        await self.execute_raw_sql(enable_triggers_sql)

        logger.info('triggers enabled.')


class DBTable(object):
    """Класс для работы с таблицей базы данных.

    Предоставляет методы для:
    - Управления колонками таблицы
    - Работы с первичными и внешними ключами
    - Управления последовательностями
    - Отслеживания процесса переноса данных
    """

    __slots__ = (
        'name',
        'full_count',
        'max_pk',
        'columns',
        '_is_ready_for_transferring',
        '_is_checked',
        '_key_column',
        'revert_foreign_tables',
        'need_transfer_pks',
        'transferred_pks_count',
    )

    schema = 'public'

    # Понижающее количество объектов, т.к. во время доведения могут
    # производиться действия пользователями и кол-во объектов может меняться
    inaccuracy_count = 100

    def __init__(self, name):
        """Инициализация таблицы.

        Args:
            name: Имя таблицы
        """
        self.name = name
        self.full_count = 0
        self.max_pk = 0
        self.columns: Dict[str, 'DBColumn'] = {}

        # Таблица готова к переносу
        self._is_ready_for_transferring = False

        # Таблица проверена в процессе сбора значений
        self._is_checked: bool = False

        self._key_column = None

        # Словарь обратных таблиц, где ключ - обратная таблица,
        # а значения - множество колонок базы данных
        self.revert_foreign_tables: Dict[DBTable, Set[DBColumn]] = defaultdict(set)

        # Первичные ключи таблицы для переноса
        self.need_transfer_pks = set()

        self.transferred_pks_count = 0

    def __repr__(self):
        """Получение строкового представления таблицы для отладки.

        Returns:
            str: Строка с именем таблицы и количеством колонок
        """
        return (
            f'<{self.__class__.__name__} @name="{self.name}" '
            f'@with_fk="{self.with_fk}" '
            f'@with_key_column="{self.with_key_column}" '
            f'@with_self_fk="{self.with_self_fk}" '
            f'@need_transfer_pks_count="{len(self.need_transfer_pks)}" >'
        )

    def __str__(self):
        """Получение строкового представления таблицы.

        Returns:
            str: Имя таблицы с дополнительной информацией
        """
        return self.__repr__()

    def __eq__(self, other):
        """Сравнение таблиц.

        Args:
            other: Другая таблица для сравнения

        Returns:
            bool: True, если таблицы имеют одинаковые имена
        """
        return self.name == other.name

    def __hash__(self):
        """Получение хэша таблицы.

        Returns:
            int: Хэш имени таблицы
        """
        return hash(self.name)

    @property
    @lru_cache()
    def primary_key(self):
        """Получение первичного ключа таблицы.

        Возвращает первую колонку с ограничением PRIMARY KEY,
        исключая колонки с типом данных DATE. Это необходимо, до тех пор, пока не
        будет поддержки составных первичных ключей

        Returns:
            DBColumn: Колонка первичного ключа или None
        """
        primary_key_columns = list(
            filter(
                lambda c: ConstraintTypesEnum.PRIMARY_KEY in c.constraint_type and c.data_type != 'date',
                self.columns.values(),
            )
        )

        if primary_key_columns:
            return primary_key_columns[0]

    @property
    def is_ready_for_transferring(self) -> bool:
        """Проверка готовности таблицы к переносу данных.

        Returns:
            bool: True, если таблица готова к переносу
        """
        return self._is_ready_for_transferring

    @is_ready_for_transferring.setter
    def is_ready_for_transferring(self, is_ready_for_transferring):
        """Установка флага готовности таблицы к переносу.

        Args:
            is_ready_for_transferring: Новое значение флага
        """
        self._is_ready_for_transferring = is_ready_for_transferring

    @property
    def is_full_prepared(self):
        """Проверка готовности таблицы к переносу данных.

        Returns:
            bool: True, если таблица готова к переносу
        """
        logger.debug(
            f'table - {self.name} -- count table records {self.full_count} and '
            f'need transfer pks {len(self.need_transfer_pks)}'
        )

        if len(self.need_transfer_pks) >= self.full_count - self.inaccuracy_count:
            logger.info(f'table {self.name} full transferred')

            return True

    @property
    @lru_cache()
    def with_fk(self):
        """Проверка наличия внешних ключей в таблице.

        Returns:
            bool: True, если таблица имеет внешние ключи
        """
        return bool(self.foreign_keys_columns)

    @property
    @lru_cache()
    def key_column(self):
        """Получение ключевой колонки таблицы.

        Returns:
            DBColumn: Ключевая колонка или None
        """
        return self._key_column

    @property
    @lru_cache()
    def with_key_column(self):
        """Проверка наличия ключевой колонки в таблице.

        Returns:
            bool: True, если таблица имеет ключевую колонку
        """
        return bool(self.key_column)

    @property
    @lru_cache()
    def with_self_fk(self):
        """Проверка наличия внешних ключей, ссылающихся на эту же таблицу.

        Returns:
            bool: True, если таблица имеет самоссылающиеся внешние ключи
        """
        return bool(self.self_fk_columns)

    @property
    @lru_cache()
    def with_not_self_fk(self):
        """Проверка наличия внешних ключей, ссылающихся на другие таблицы.

        Returns:
            bool: True, если таблица имеет внешние ключи на другие таблицы
        """
        return bool(self.not_self_fk_columns)

    @property
    @lru_cache()
    def unique_fk_columns(self) -> List['DBColumn']:
        """Получение уникальных внешних ключей таблицы.

        Returns:
            List[DBColumn]: Список колонок с уникальными внешними ключами
        """
        return list(
            filter(
                lambda column: column.is_foreign_key and column.is_unique,
                self.not_self_fk_columns,
            )
        )

    @property
    @lru_cache()
    def foreign_keys_columns(self):
        """Получение всех колонок с внешними ключами.

        Returns:
            List[DBColumn]: Список колонок с внешними ключами
        """
        return list(
            filter(
                lambda column: column.is_foreign_key,
                self.columns.values(),
            )
        )

    @property
    @lru_cache()
    def self_fk_columns(self):
        """Получение колонок с внешними ключами, ссылающимися на эту же таблицу.

        Returns:
            List[DBColumn]: Список самоссылающихся колонок
        """
        return list(
            filter(
                lambda column: column.is_self_fk,
                self.columns.values(),
            )
        )

    @property
    @lru_cache()
    def not_self_fk_columns(self) -> List['DBColumn']:
        """Получение колонок с внешними ключами, ссылающимися на другие таблицы.

        Returns:
            List[DBColumn]: Список колонок с внешними ключами на другие таблицы
        """
        return list(
            filter(
                lambda c: c.is_foreign_key and not c.is_self_fk,
                self.columns.values(),
            )
        )

    @property
    @lru_cache()
    def fk_columns_with_key_column(self) -> List['DBColumn']:
        """Получение колонок с внешними ключами на таблицы с ключевой колонкой.

        Returns:
            List[DBColumn]: Список колонок с внешними ключами на таблицы с ключевой колонкой
        """
        return list(
            filter(
                lambda column: column.constraint_table.with_key_column,
                self.not_self_fk_columns,
            )
        )

    @property
    @lru_cache()
    def unique_fk_columns_with_key_column(self) -> List['DBColumn']:
        """Получение уникальных внешних ключей на таблицы с ключевой колонкой.

        Returns:
            List[DBColumn]: Список уникальных колонок с внешними ключами на таблицы с ключевой колонкой
        """
        return list(set(self.unique_fk_columns).intersection(self.fk_columns_with_key_column))

    @property
    @lru_cache
    def fk_columns_tables_with_fk_columns_with_key_column(self) -> List['DBColumn']:
        """Получение внешних ключей на таблицы, которые имеют внешние ключи на таблицы с ключевой колонкой.

        Returns:
            List[DBColumn]: Список колонок с внешними ключами на таблицы,
                          имеющие внешние ключи на таблицы с ключевой колонкой
        """
        columns = []

        for column in self.not_self_fk_columns:
            constraint_table_fk_columns = column.constraint_table.not_self_fk_columns

            for constraint_column in constraint_table_fk_columns:
                if constraint_column.constraint_table.with_key_column:
                    columns.append(column)

        return columns

    @property
    @lru_cache
    def unique_fk_columns_tables_with_fk_columns_with_key_column(self) -> List['DBColumn']:
        """Получение уникальных внешних ключей на таблицы, которые имеют внешние ключи на таблицы с ключевой колонкой.

        Returns:
            List[DBColumn]: Список уникальных колонок с внешними ключами на таблицы,
                          имеющие внешние ключи на таблицы с ключевой колонкой
        """
        columns = []

        for column in self.unique_fk_columns:
            constraint_table_fk_columns = column.constraint_table.not_self_fk_columns

            for constraint_column in constraint_table_fk_columns:
                if constraint_column.constraint_table.with_key_column:
                    columns.append(column)

        return columns

    @property
    def is_checked(self) -> bool:
        """Проверка статуса проверки таблицы.

        Returns:
            bool: True, если таблица была проверена
        """
        return self._is_checked

    @is_checked.setter
    def is_checked(self, value):
        """Установка статуса проверки таблицы.

        Args:
            value: Новое значение статуса проверки
        """
        self._is_checked = value

    @property
    @lru_cache()
    def highest_priority_fk_columns(self) -> List['DBColumn']:
        """Получение внешних ключей с наивысшим приоритетом.

        Приоритет определяется в следующем порядке:
        1. Уникальные внешние ключи на таблицы с ключевой колонкой
        2. Уникальные внешние ключи на таблицы с внешними ключами на таблицы с ключевой колонкой
        3. Внешние ключи на таблицы с ключевой колонкой
        4. Внешние ключи на таблицы с внешними ключами на таблицы с ключевой колонкой

        Returns:
            List[DBColumn]: Список колонок с наивысшим приоритетом
        """
        if self.unique_fk_columns_with_key_column:
            fk_columns = self.unique_fk_columns_with_key_column
        elif self.unique_fk_columns_tables_with_fk_columns_with_key_column or self.fk_columns_with_key_column:
            fk_columns = []
            if self.unique_fk_columns_tables_with_fk_columns_with_key_column:
                fk_columns.extend(self.unique_fk_columns_tables_with_fk_columns_with_key_column)
            if self.fk_columns_with_key_column:
                fk_columns.extend(self.fk_columns_with_key_column)
        elif self.fk_columns_tables_with_fk_columns_with_key_column:
            fk_columns = self.fk_columns_tables_with_fk_columns_with_key_column
        else:
            fk_columns = self.not_self_fk_columns

        return fk_columns

    def update_need_transfer_pks(
        self,
        need_transfer_pks: Iterable[Union[int, str]],
    ):
        """Обновление множества идентификаторов записей для переноса.

        Args:
            need_transfer_pks: Итерируемый объект с идентификаторами для переноса
        """
        self.need_transfer_pks.update(need_transfer_pks)

        del need_transfer_pks

    async def append_column(
        self,
        column_name: str,
        data_type: str,
        ordinal_position: int,
        constraint_table: Optional['DBTable'],
        constraint_type: str,
    ):
        """Добавление колонки в таблицу.

        Args:
            column_name: Имя колонки
            data_type: Тип данных колонки
            ordinal_position: Позиция колонки в таблице
            constraint_table: Таблица, на которую ссылается внешний ключ
            constraint_type: Тип ограничения колонки

        Returns:
            None
        """
        if column_name in self.columns:
            column: DBColumn = await self.get_column_by_name(column_name)

            if constraint_type:
                await column.add_constraint_type(constraint_type)

                if constraint_type == ConstraintTypesEnum.FOREIGN_KEY:
                    column.constraint_table = constraint_table
                    DBColumn.is_foreign_key.fget.cache_clear()
        else:
            # postgresql возврщает тип array вместо integer array
            if data_type == 'ARRAY':
                data_type = 'integer array'

            column = DBColumn(
                column_name=column_name,
                table_name=self.name,
                data_type=data_type,
                ordinal_position=ordinal_position,
                constraint_table=constraint_table,
                constraint_type=constraint_type,
            )

            self.columns[column_name] = column

        if not self._key_column and column.is_key_column:
            self._key_column = column

        if column.is_foreign_key:
            try:
                column.constraint_table.revert_foreign_tables[self].add(column)
            except AttributeError:
                traceback_ = '\n'.join(traceback.format_stack())
                message = f'Wrong foreign key column {column}.\n{traceback_}'

                raise AttributeError(message)

        return column

    async def get_column_by_name(self, column_name):
        """Get table column by name."""
        return self.columns.get(column_name)

    def get_columns_by_constraint_types_table_name(
        self,
        table_name: str,
        constraint_types: Optional[Iterable[str]] = None,
    ) -> List['DBColumn']:
        """Получение колонок по имени таблицы ограничения и типам ограничений.

        Args:
            table_name: Имя таблицы ограничения
            constraint_types: Список типов ограничений для фильтрации

        Returns:
            List[DBColumn]: Список колонок, удовлетворяющих условиям
        """
        return list(
            filter(
                lambda c: (
                    deep_getattr(c.constraint_table, 'name') == table_name
                    and (set(c.constraint_type).intersection(set(constraint_types)) if constraint_types else True)
                ),
                self.columns.values(),
            )
        )

    def get_columns_list_str_commas(self):
        """Получение списка имен колонок через запятую.

        Returns:
            str: Строка с именами колонок в кавычках, разделенными запятыми
        """
        return ', '.join(
            map(
                lambda c: f'"{c.name}"',
                sorted(self.columns.values(), key=lambda c: c.ordinal_position),
            )
        )

    def get_columns_list_with_types_str_commas(self):
        """Получение списка колонок с их типами через запятую.

        Returns:
            str: Строка с именами колонок и их типами, разделенными запятыми
        """
        return ', '.join(
            map(
                lambda c: f'"{c.name}" {c.data_type}',
                sorted(self.columns.values(), key=lambda c: c.ordinal_position),
            )
        )

    async def set_max_sequence(self, dst_pool: Pool):
        """Установка максимального значения последовательности для первичного ключа.

        Args:
            dst_pool: Пул соединений с базой данных

        Returns:
            None
        """
        async with dst_pool.acquire() as connection:
            try:
                get_serial_sequence_sql = SQLRepository.get_serial_sequence_sql(
                    table_name=self.name,
                    pk_column_name=self.primary_key.name,
                )
            except AttributeError:
                logger.warning(f'AttributeError --- {self.name} --- {get_serial_sequence_sql} --- set_max_sequence')
                return

            try:
                serial_seq_name = await connection.fetchrow(get_serial_sequence_sql)
            except InvalidNameError:
                logger.error(f'InvalidNameError --- {self.name} --- {get_serial_sequence_sql} --- set_max_sequence')
                return

            if serial_seq_name and serial_seq_name[0]:
                serial_seq_name = serial_seq_name[0]

                max_val = self.max_pk + 100000

                set_sequence_val_sql = SQLRepository.get_set_sequence_value_sql(
                    seq_name=serial_seq_name,
                    seq_val=max_val,
                )

                await connection.execute(set_sequence_val_sql)


class DBColumn(object):
    """Класс для работы с колонкой таблицы базы данных.

    Предоставляет методы для:
    - Управления типами данных и ограничениями
    - Проверки типов ограничений (первичный ключ, внешний ключ, уникальность)
    - Работы с внешними ключами и ссылками на другие таблицы
    """

    __slots__ = (
        'name',
        'table_name',
        'data_type',
        'ordinal_position',
        'constraint_table',
        'constraint_type',
    )

    def __init__(
        self,
        column_name: str,
        table_name: str,
        data_type: str,
        ordinal_position: int,
        constraint_table: Optional[DBTable] = None,
        constraint_type: Optional[str] = None,
    ):
        """Инициализация колонки.

        Args:
            column_name: Имя колонки
            table_name: Имя таблицы, которой принадлежит колонка
            data_type: Тип данных колонки
            ordinal_position: Позиция колонки в таблице
            constraint_table: Таблица, на которую ссылается внешний ключ
            constraint_type: Тип ограничения колонки.
        """
        assert column_name, None
        assert table_name, None

        self.name = column_name
        self.table_name = table_name
        self.data_type = data_type or ''
        self.ordinal_position = ordinal_position or 0
        self.constraint_table = constraint_table
        self.constraint_type = []

        if constraint_type:
            self.constraint_type.append(constraint_type)

    def __repr__(self):
        """Получение строкового представления колонки для отладки.

        Returns:
            str: Строка с информацией о колонке
        """
        return (
            f'< {self.__class__.__name__} @name="{self.name}" '
            f'@table_name="{self.table_name}" '
            f'@data_type="{self.data_type}" '
            f'@ordinal_position="{self.ordinal_position}" '
            f'@is_foreign_key="{self.is_foreign_key}" '
            f'@foreign_table_name="{deep_getattr(self.constraint_table, "name", " - ")}" '
            f'@constraint_types="{make_str_from_iterable(self.constraint_type)}">'
        )

    def __str__(self):
        """Получение строкового представления колонки.

        Returns:
            str: Имя колонки c информацией о колонке
        """
        return self.__repr__()

    @property
    @lru_cache()
    def is_foreign_key(self):
        """Проверка, является ли колонка внешним ключом.

        Returns:
            bool: True, если колонка является внешним ключом
        """
        return ConstraintTypesEnum.FOREIGN_KEY in self.constraint_type

    @property
    @lru_cache()
    def is_primary_key(self):
        """Проверка, является ли колонка первичным ключом.

        Returns:
            bool: True, если колонка является первичным ключом
        """
        return ConstraintTypesEnum.PRIMARY_KEY in self.constraint_type

    @property
    @lru_cache()
    def is_unique(self):
        """Проверка, является ли колонка уникальной.

        Returns:
            bool: True, если колонка имеет ограничение уникальности
        """
        return ConstraintTypesEnum.UNIQUE in self.constraint_type or (self.is_foreign_key and self.is_primary_key)

    @property
    @lru_cache()
    def is_key_column(self):
        """Проверка, является ли колонка ключевой.

        Returns:
            bool: True, если имя колонки входит в список KEY_COLUMN_NAMES
        """
        return self.name in KEY_COLUMN_NAMES or deep_getattr(self.constraint_table, 'name') == KEY_TABLE_NAME

    @property
    def is_self_fk(self):
        """Проверка, является ли колонка самоссылающимся внешним ключом.

        Returns:
            bool: True, если колонка ссылается на свою же таблицу
        """
        return self.is_foreign_key and deep_getattr(self.constraint_table, 'name') == self.table_name

    def get_column_name_with_type(self):
        """Получение строки с именем колонки и ее типом данных.

        Returns:
            str: Строка вида '"column_name" data_type'
        """
        return f'{self.name} {self.data_type}'

    async def add_constraint_type(self, constraint_type):
        """Добавление типа ограничения к колонке.

        Args:
            constraint_type: Тип ограничения для добавления

        Returns:
            None
        """
        self.constraint_type.append(constraint_type)
