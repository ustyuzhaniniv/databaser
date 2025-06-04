import logging
import operator
import os
import sys
import uuid
from collections import (
    defaultdict,
    namedtuple,
)
from datetime import (
    datetime,
)
from itertools import (
    chain,
    islice,
)
from typing import (
    Any,
    Iterable,
    List,
    Tuple,
    Union,
)

logger = logging.getLogger('asyncio')

sh = logging.StreamHandler(
    stream=sys.stdout,
)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

sh.setFormatter(formatter)
logger.addHandler(sh)

DBConnectionParameters = namedtuple(
    typename='DBConnectionParameters',
    field_names=[
        'host',
        'port',
        'schema',
        'dbname',
        'user',
        'password',
    ],
)


def strtobool(value: str) -> bool:
    """Преобразование строкового представления истинности в булево значение.

    Args:
        value: Строковое значение для преобразования

    Returns:
        True, если значение является одним из: 'y', 'yes', 't', 'true', 'on', '1';
        False в противном случае.

    Пример:
        >>> strtobool('yes')
        True
        >>> strtobool('no')
        False
    """
    value = value.lower()

    if value in ('y', 'yes', 't', 'true', 'on', '1'):
        return True

    return False


def make_str_from_iterable(
    iterable: Iterable[Any],
    with_quotes: bool = False,
    quote: str = '"',
) -> str:
    """Вспомогательная функция для преобразования итерируемого объекта к строке.

    Args:
        iterable: итерируемый объект
        with_quotes: необходимость оборачивания элементов в кавычки
        quote: вид кавычки

    Returns:
        Сформированная строка
    """
    iterable_str = ''

    if iterable:
        if with_quotes:
            iterable_strs = map(lambda item: f'{quote}{item}{quote}', iterable)
        else:
            iterable_strs = map(str, iterable)

        iterable_str = ', '.join(iterable_strs)

    return iterable_str


def dates_to_string(dates_list: Iterable[datetime], format_: str = '%Y-%m-%d %H:%M:%S'):
    """Преобразование дат, содержащихся в итерируемом объекте."""
    return ', '.join(map(lambda date_: f'{date_:{format_}}', dates_list))


# Именованный кортеж содержащий результат работы функции топологической сортировки
Results = namedtuple('Results', ['sorted', 'cyclic'])


def topological_sort(
    dependency_pairs: Iterable[Union[str, Tuple[str, str]]],
):
    """Сортировка по степени зависимости.

    print( topological_sort('aa'.split()) )
    print( topological_sort('ah bg cf ch di ed fb fg hd he ib'.split()) )

    Спасибо Raymond Hettinger
    """
    num_heads = defaultdict(int)  # num arrows pointing in
    tails = defaultdict(list)  # list of arrows going out
    heads = []  # unique list of heads in order first seen

    for h, t in dependency_pairs:
        num_heads[t] += 1
        if h in tails:
            tails[h].append(t)
        else:
            tails[h] = [t]
            heads.append(h)

    ordered = [h for h in heads if h not in num_heads]
    for h in ordered:
        for t in tails[h]:
            num_heads[t] -= 1
            if not num_heads[t]:
                ordered.append(t)

    cyclic = [n for n, heads in num_heads.items() if heads]

    return Results(ordered, cyclic)


def make_chunks(
    iterable: Iterable[Any],
    size: int,
    is_list: bool = False,
):
    """Разделение итерируемого объекта на части указанного в параметрах размера.

    Args:
        iterable: итерируемый объект
        size: количество объектов в части
        is_list: преобразовать к спискам формируемые части
    """
    iterator = iter(iterable)

    for first in iterator:
        yield (
            list(chain([first], islice(iterator, size - 1))) if is_list else chain([first], islice(iterator, size - 1))
        )


def deep_getattr(object_, attribute_: str, default=None):
    """Получить значение атрибута с любого уровня цепочки вложенных объектов.

    Args:
        object_: объект, у которого ищется значение атрибута
        attribute_: атрибут, значение которого необходимо получить (указывается полная цепочка, т.е. 'attr1.attr2.atr3')
        default: значение по умолчанию

    Returns:
        Значение указанного атрибута или значение по умолчанию, если
        атрибут не был найден
    """
    try:
        value = operator.attrgetter(attribute_)(object_)
    except AttributeError:
        value = default

    return value


def get_str_environ_parameter(
    name: str,
    default: str = '',
) -> str:
    """Получение значения параметра из переменных окружения, имеющего строковое значение.

    Args:
        name: имя переменной окружения
        default: значение по-умолчанию

    Returns:
        Полученное значение
    """
    return os.environ.get(name, default).strip()


def get_int_environ_parameter(
    name: str,
    default: int = 0,
) -> int:
    """Получение целочисленного значения параметра из переменных окружения.

    Args:
        name: Имя переменной окружения
        default: Значение по умолчанию, если параметр не найден или не может быть преобразован в число

    Returns:
        int: Целочисленное значение параметра

    Raises:
        ValueError: Если значение параметра не может быть преобразовано в целое число
    """
    return int(os.environ.get(name, default))


def get_bool_environ_parameter(
    name: str,
    default: bool = False,
) -> bool:
    """Получение булева значения параметра из переменных окружения.

    Args:
        name: Имя переменной окружения
        default: Значение по умолчанию, если параметр не найден

    Returns:
        bool: Булево значение параметра. True для значений 'y', 'yes', 't', 'true', 'on', '1'
    """
    parameter_value = os.environ.get(name)

    if parameter_value:
        parameter_value = bool(strtobool(parameter_value))
    else:
        parameter_value = default

    return parameter_value


def get_iterable_environ_parameter(
    name: str,
    separator: str = ',',
    type_=str,
) -> Tuple[str]:
    """Получение кортежа значений из переменной окружения.

    Args:
        name: Имя переменной окружения
        separator: Разделитель значений в строке
        type_: Тип данных для преобразования значений

    Returns:
        Tuple[str]: Кортеж значений, преобразованных к указанному типу
    """
    return tuple(map(type_, filter(None, os.environ.get(name, '').replace(' ', '').split(separator))))


def get_extensible_iterable_environ_parameter(
    name: str,
    separator: str = ',',
    type_=str,
) -> List[str]:
    """Получение расширяемого списка значений из переменной окружения.

    Args:
        name: Имя переменной окружения
        separator: Разделитель значений в строке
        type_: Тип данных для преобразования значений

    Returns:
        List[str]: Список значений, преобразованных к указанному типу
    """
    return list(map(type_, filter(None, os.environ.get(name, '').replace(' ', '').split(separator))))


def add_file_handler_logger(
    directory_path: str,
    file_name: str,
) -> None:
    """Добавление обработчика для записи логов в файл.

    Args:
        directory_path: Путь к директории для сохранения лог-файла
        file_name: Имя лог-файла

    Returns:
        None

    Raises:
        OSError: При невозможности создать директорию или файл для логов
    """
    if directory_path:
        file_name = f'{file_name}_{uuid.uuid4().hex[:8]}'
        fh = logging.FileHandler(f'{directory_path}/{file_name}.log')
        fh.setFormatter(formatter)
        logger.addHandler(fh)
