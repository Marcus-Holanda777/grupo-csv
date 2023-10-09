from itertools import (
    chain,
    starmap,
    count,
    groupby
)
from typing import Any
from functools import partial
from operator import itemgetter
from dateutil.parser import parse
import csv
import shelve
from pathlib import Path

# NOTE: Cria a pasta dos dados
RAIZ_DATA = Path('data')
RAIZ_DATA.mkdir(exist_ok=True)


def trata_linha(row: list) -> tuple:
    """Converter as linhas de dados
    para os tipos 

    >> str, int, float ou datetime

    Argumentos:
        row (list): Lista de dados

    Retorno:
        tuple: Uma tupla com os tipos convertidos
    """
    def convert(p: int, itm: str) -> Any:
        """
         O item na posição 4 da lista
         é o CPF e não deve ser convertido
         para inteiro, deve ficar como str
        """
        if p == 4:
            return itm.strip()

        try:
            return (
                float(itm.replace(',', '.'))
                if ',' in itm
                else int(itm)
            )
        except:
            return (
                parse(itm).replace(day=1)
                if '/' in itm
                else itm.strip()
            )

    return tuple(
        starmap(convert, enumerate(row))
    )


def soma_key(
    key: Any, 
    value: iter, 
    column: int
) -> tuple:
    """
      Soma a quantidade de cupons
      com base no CPF do cliente
    """

    return (key, sum(row[column] for row in value))


def return_data(rowns: iter) -> list[Any]:
    """
      Recebe os dados.
      ordena por CPF, agrupa por CPD,
      aplica a funcao de somar os cupons
      e retorna uma lista, com o grupo
    """
    data = list(
        starmap(
            partial(soma_key, column = 2),
            groupby(
                sorted(
                    map(trata_linha, rowns), 
                    key=itemgetter(4)
                ),
                key=itemgetter(4)
            )
        )
    )

    return data


def update_shelve(
    data: list[Any],
    name: int
) -> None:
    """
       Quarda o objeto de dados
       no estado, apos o agrupamento
    """

    out_path = RAIZ_DATA / f'{name}'
    out_path.mkdir(exist_ok=True)

    with shelve.open(
        out_path / f'{name}', 
    ) as db:
        
        db['data'] = data 


def select_shelve(file: Path) -> list:
    """
       Leitura e retorno dos grupos
    """
    cam = str(file)[:-4]
    with shelve.open(cam) as db: 
        data = db['data']
    return data


def open_file(
    *,
    file: str,
    porcao: int
) -> list[Any]:
    """
       Com base na porcao de dados definida
       trata os dados e salva em formato binario
    """
    
    cont = 0

    def add_porcao(values, cont) -> None:
        update_shelve(return_data(values), cont)
        print(f'Dados: {cont}')

    with open(file, newline='') as f:
        rowns = csv.reader(f, delimiter=';')
        next(rowns) # retirar cabecalho
        
        values = []
        for pos, value in enumerate(rowns, 1):

           values.append(value)
           if pos % porcao == 0:
               cont += len(values)
               add_porcao(values, cont)
               values = []
        
        # NOTE: O restante da porcao
        cont += len(values)
        add_porcao(values, cont)

    return list(
        chain.from_iterable(
            map(
                select_shelve,
                RAIZ_DATA.glob("**/*.dat")
            )
        )
    )


def resumo(
    container: list[Any],
    /,
    *,
    filter_days: int
) -> list[Any]:
    """
       Recebe todos os grupos
       e gera o resumo final aplicando um
       filtro na quantidade de cupons
       por CPF
    """

    dados = (
        filter(lambda x: x[1] >= filter_days,
            starmap(
                partial(soma_key, column = 1),
                    groupby(
                        sorted(container, key=itemgetter(0)),
                        key = itemgetter(0)
                )
            )
        )
    )

    return list(dados)


def count_rows(file: str) -> int:
    """
      Total das linhas do arquivo
    """
    con = count()
    with open(file, 'r') as f:
        for __ in f:
            tot = next(con)
    return tot


if __name__ == '__main__':
    arq = 'vendas geral.csv'
    lins_tot = count_rows(arq)

    print(f'Total de linhas: {lins_tot}')
    
    dados = resumo(
        open_file(
            file=arq,
            porcao=10_000_000
        ),
        filter_days=365
    )

    print(f'Resumo retorno: {len(dados)}')