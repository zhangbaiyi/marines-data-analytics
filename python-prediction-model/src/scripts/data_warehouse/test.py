from datetime import date

from src.scripts.data_warehouse.access import convert_jargons, getMetricByID, getSiteByID, query_facts
from src.scripts.data_warehouse.models.warehouse import Session
from src.utils.logging import LOGGER

if __name__ == "__main__":
    session = Session()
    LOGGER.info(
        """What is the total sales of 
                    HHM MCX MAIN STORE (Henderson Hall Main Store) in 
                        December 2024? """
    )
    LOGGER.info(
        query_facts(session=session, metric_id=1, group_names=[
                    "1100"], period_levels=[2], exact_date=date(2024, 12, 1))
    )
    LOGGER.info(
        """What is the total sales of 
                    HHM MCX MAIN STORE (Henderson Hall Main Store) 
                        in the first three days of 2025? """
    )
    LOGGER.info(
        query_facts(
            session=session,
            metric_id=1,
            group_names=["1100"],
            period_levels=[2],
            date_from=date(2025, 1, 1),
            date_to=date(2025, 1, 3),
        )
    )
    LOGGER.info(
        """What is the total sales of 
                                        HHM MCX MAIN STORE (Henderson Hall Main Store) 
                          compared with CLM MCX MAIN STORE (Camp Lejelle Main Store)
                                                 On January 1st 2025? """
    )
    LOGGER.info(
        query_facts(
            session=session, metric_id=1, group_names=["1100", "5100"], period_levels=[2], exact_date=date(2025, 1, 1)
        )
    )
    LOGGER.info(
        """What is the total sales of 
                        HHM MCX MAIN STORE (Henderson Hall Main Store) 
                                    On 4Q24 and 1Q25? """
    )
    LOGGER.info(
        query_facts(
            session=session,
            metric_id=1,
            group_names=["1100"],
            period_levels=[3],
            date_from=date(2024, 10, 1),
            date_to=date(2025, 1, 1),
        )
    )
    LOGGER.info(
        """What is the total sales 
                           and [Another metric] of 
                                    HHM MCX MAIN STORE (Henderson Hall Main Store) 
                                        On Jan 1st 2025? """
    )
    LOGGER.info(
        query_facts(
            session=session, metric_ids=[1, 2], group_names=["1100"], period_levels=[1], exact_date=date(2025, 1, 1)
        )
    )

    LOGGER.info(getMetricByID(session=session, metric_id=1))
    LOGGER.info(getSiteByID(session=session, site_id="1100"))
    LOGGER.info(
        convert_jargons(
            session=session,
            df=query_facts(
                session=session, metric_id=1, group_names=["1100"], period_levels=[2], exact_date=date(2024, 12, 1)
            ),
        )
    )
    LOGGER.info(
        convert_jargons(
            session=session,
            df=query_facts(
                session=session,
                metric_id=1,
                group_names=["1100"],
                period_levels=[1],
                date_from=date(2025, 1, 1),
                date_to=date(2025, 1, 3),
            ),
        )
    )
    LOGGER.info(
        convert_jargons(
            session=session,
            df=query_facts(
                session=session, metric_id=1, group_names=["1100"], period_levels=[3], exact_date=date(2024, 10, 1)
            ),
        )
    )
    LOGGER.info(
        convert_jargons(
            session=session,
            df=query_facts(
                session=session,
                metric_ids=[1],
                group_names=["1100", "5206", "2301"],
                period_levels=[4],
                exact_date=date(2025, 1, 1),
            ),
        )
    )
    LOGGER.info(
        convert_jargons(
            session=session,
            df=query_facts(
                session=session,
                metric_ids=[1],
                group_names=["1100"],
                period_levels=[2],
                exact_date=date(2024,10,1)
            ),
        )
    )


# # [Facts(metric_id=('Total Sales, Total sales of Marine Mart or Main Store'), group_name='HHM MCX MAIN STORE,HENDERSON HALL,MAIN STORE', value=2224948.54, 20241201->20241231]


# [Facts(id=4076, metric_id=1, group_name='1100', value=2224948.54, date=datetime.date(2024, 12, 1), period_level=2, record_inserted_date=datetime.datetime(2025, 3, 6, 7, 5, 46, 865558))]
# [Facts(metric_id=('Total Sales, Total sales of Marine Mart or Main Store'), group_name='HHM MCX MAIN STORE,HENDERSON HALL,MAIN STORE', value=2224948.54, 20241201->20241231]
# ('Total Sales, Total sales of Marine Mart or Main Store', 'HHM MCX MAIN STORE,HENDERSON HALL,MAIN STORE', 2, '2024-12-01')
# ('Total Sales, Total sales of Marine Mart or Main Store', 'HHM MCX MAIN STORE,HENDERSON HALL,MAIN STORE', 2024-12-01 -> 2024-12-31'')
#
