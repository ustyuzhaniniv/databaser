import asyncio
from abc import (
    ABCMeta,
    abstractmethod,
)
from typing import (
    Dict,
    List,
    Set,
    Tuple,
    Type,
    Union,
)

from asyncpg import (
    Record,
)
from prettytable import (
    PrettyTable,
)

from databaser.core.db_entities import (
    BaseDatabase,
    DBTable,
    DstDatabase,
    SrcDatabase,
)
from databaser.core.helpers import (
    logger,
    make_str_from_iterable,
)
from databaser.core.loggers import (
    StatisticManager,
)


class BaseValidator(metaclass=ABCMeta):
    """Базовый класс для создания валидаторов данных."""

    def __init__(
        self,
        dst_database: DstDatabase,
        src_database: SrcDatabase,
        statistic_manager: StatisticManager,
        key_column_values: Set[int],
    ):
        """Инициализация базового валидатора.

        Args:
            dst_database: Целевая база данных
            src_database: Исходная база данных
            statistic_manager: Менеджер статистики
            key_column_values: Множество значений ключевой колонки
        """
        self._dst_database = dst_database
        self._src_database = src_database
        self._statistic_manager = statistic_manager
        self._key_column_ids = key_column_values

    @abstractmethod
    async def validate(self) -> Tuple[bool, str]:
        """Метод для валидации данных.

        Returns:
            Tuple[bool, str]: Кортеж из флага валидности и сообщения
        """


class TablesWithKeyColumnValidator(BaseValidator):
    """Валидатор данных для таблиц с ключевой колонкой."""

    GET_TABLE_KEY_COLUMN_IDS_SQL = """
        SELECT DISTINCT "{key_column_name}"
        FROM "{table_name}";
        """

    def __init__(
        self,
        *args,
        **kwargs,
    ):
        """Инициализация валидатора таблиц с ключевой колонкой.

        Args:
            *args: Позиционные аргументы для базового класса
            **kwargs: Именованные аргументы для базового класса
        """
        super().__init__(
            *args,
            **kwargs,
        )

        self._validation_result: List[str] = []

    async def _validate_table_data(
        self,
        table: DBTable,
    ):
        """Валидация значений ключевой колонки в таблице.

        Args:
            table: Таблица для валидации

        Returns:
            None. Добавляет сообщения об ошибках в self._validation_result
        """
        get_table_key_column_ids_sql = self.GET_TABLE_KEY_COLUMN_IDS_SQL.format(
            key_column_name=table.key_column.name,
            table_name=table.name,
        )

        async with self._dst_database.connection_pool.acquire() as connection:
            key_column_ids_records: List[Record] = await connection.fetch(
                query=get_table_key_column_ids_sql,
            )

        if key_column_ids_records:
            key_column_ids = {str(record.get(table.key_column.name)) for record in key_column_ids_records}

            difference = key_column_ids.difference(self._key_column_ids)

            if difference:
                wrong_key_column_ids = make_str_from_iterable(
                    iterable=difference,
                    with_quotes=True,
                )

                self._validation_result.append(
                    f'Wrong key column "{table.key_column.name}" ids found '
                    f'in table "{table.name}" - {wrong_key_column_ids}!'
                )

    async def validate(self):
        """Валидация всех таблиц с ключевой колонкой.

        Returns:
            Tuple[bool, str]: Кортеж из флага валидности и сообщения с результатами проверки
        """
        coroutines = [
            asyncio.create_task(
                self._validate_table_data(
                    table=table,
                )
            )
            for table in self._dst_database.tables_with_key_column
        ]

        if coroutines:
            await asyncio.wait(coroutines)

        if self._validation_result:
            is_valid = False
            message = '\n'.join(self._validation_result)
        else:
            is_valid = True
            message = 'Validation was successful.'

        return is_valid, message


class ValidatorManager:
    """Менеджер для запуска валидаторов после переноса данных в целевую базу данных."""

    validator_classes = (TablesWithKeyColumnValidator,)

    def __init__(
        self,
        dst_database: DstDatabase,
        src_database: SrcDatabase,
        statistic_manager: StatisticManager,
        key_column_values: Set[int],
    ):
        """Инициализация менеджера валидаторов.

        Args:
            dst_database: Целевая база данных
            src_database: Исходная база данных
            statistic_manager: Менеджер статистики
            key_column_values: Множество значений ключевой колонки
        """
        self._dst_database = dst_database
        self._src_database = src_database
        self._statistic_manager = statistic_manager
        self._key_column_ids = ['None', *list(map(str, key_column_values))]

        self._validation_result: Dict[str, Tuple[bool, str]] = {}

    def _print_result(self):
        """Вывод результатов валидации в виде таблицы."""
        result_table = PrettyTable()

        result_table.field_names = ['Validator', 'Is valid', 'Message']

        for validator_class_name, (is_valid, message) in self._validation_result.items():
            result_table.add_row(
                (
                    validator_class_name,
                    is_valid,
                    message,
                )
            )

        logger.info(result_table)

    async def _run_validator(
        self,
        validator_class: Type[BaseValidator],
        **parameters: Dict[str, Union[BaseDatabase, StatisticManager]],
    ):
        """Запуск отдельного валидатора.

        Args:
            validator_class: Класс валидатора для запуска
            **parameters: Параметры для инициализации валидатора

        Returns:
            None. Сохраняет результаты в self._validation_result
        """
        validator = validator_class(**parameters)

        is_valid, message = await validator.validate()

        self._validation_result[validator_class.__name__] = is_valid, message

    async def _run_validators(self):
        """Запуск всех зарегистрированных валидаторов.

        Returns:
            None. Результаты сохраняются в self._validation_result
        """
        parameters = {
            'dst_database': self._dst_database,
            'src_database': self._src_database,
            'statistic_manager': self._statistic_manager,
            'key_column_values': self._key_column_ids,
        }

        coroutines = [
            self._run_validator(
                validator_class=validator_class,
                **parameters,
            )
            for validator_class in self.validator_classes
        ]

        if coroutines:
            await asyncio.gather(*coroutines)

    async def validate(self) -> bool:
        """Запуск процесса валидации и вывод результатов.

        Returns:
            bool: Флаг валидности
        """
        await self._run_validators()
        self._print_result()

        is_valid: bool = all([item[0] for item in self._validation_result.values()])

        return is_valid
