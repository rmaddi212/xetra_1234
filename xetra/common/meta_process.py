from datetime import datetime,timedelta
import collections
import pandas as pd
from xetra.common.s3 import S3BucketConnector
from xetra.common.constants import MetaProcessFormat
from xetra.common.custom_exceptions import WrongMetaFileException

"""Methods for processing the meta file"""
class MetaProcess():
    """
    Class for working with the meta file
    """

    @staticmethod
    def update_meta_file(extract_date_list: list,meta_key: str, s3_bucket_meta: S3BucketConnector):
        df_new = pd.DataFrame(columns=[MetaProcessFormat.META_SOURCE_DATE_COL.value,
                                       MetaProcessFormat.META_PROCESS_COL.value])
        #                                       
        df_new[MetaProcessFormat.META_SOURCE_DATE_COL.value] = extract_date_list
        df_new[MetaProcessFormat.META_PROCESS_COL.value] = \
            datetime.today().strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)
        try:
            df_old = s3_bucket_meta.read_csv_to_df(meta_key)
            if collections.Counter(df_old.columns) != collections.Counter(df_new.columns):
                raise WrongMetaFileException
            df_all = pd.concat([df_old,df_new])
        except s3_bucket_meta.session.client('s3').exceptions.NoSuchKey:
            df_all = df_new
        s3_bucket_meta.write_df_to_s3(df_all,meta_key,MetaProcessFormat.META_FILE_FORMAT.value)
        return True

    @staticmethod
    def return_date_list(first_date: str, meta_key: str, s3_bucket_meta: S3BucketConnector):
        """
            min_date = datetime.strptime(arg_date, src_format).date() - timedelta(days=1)
            today = datetime.today().date()
            try:
                df_meta = read_csv_to_df(bucket, meta_key)
                dates = [(min_date + timedelta(days=x)) for x in range(0, (today-min_date).days + 1)]
                src_dates = set(pd.to_datetime(df_meta['source_date']).dt.date)
                dates_missing = set(dates[1:]) - src_dates
                if dates_missing:
                    min_date = min(set(dates[1:]) - src_dates) - timedelta(days=1)
                    return_dates = [date.strftime(src_format) for date in dates if date >= min_date]
                    return_min_date = (min_date + timedelta(days=1)).strftime(src_format)
                else:
                    return_dates = []
                    return_min_date = datetime(2200, 1, 1).date()
            except bucket.session.client('s3').execptions.NoSuchKey:
                return_dates = [(min_date + timedelta(days=x)).strftime(src_format) for x in range(0, (today-min_date).days + 1)]
                return_min_date = arg_date
            return return_min_date, return_dates
        """
        start = datetime.strptime(first_date, MetaProcessFormat.META_DATE_FORMAT.value).date() - timedelta(days=1)
        today = datetime.today().date()
        try:
            df_meta = s3_bucket_meta.read_csv_to_df(meta_key)
            dates = [start + timedelta(days=x) for x in range(0,(today - start).days + 1)]
            src_dates = set(pd.to_datetime(df_meta[MetaProcessFormat.META_SOURCE_DATE_COL.value]).dt.date)
            dates_missing = set(dates[1:]) - src_dates
            if dates_missing:
                min_date = min(set(dates[1:]) - src_dates) - timedelta(days=1)
                return_min_date = (min_date + timedelta(days=1)).strftime(MetaProcessFormat.META_DATE_FORMAT.value)
                return_dates = [date.strftime(MetaProcessFormat.META_DATE_FORMAT.value) for date in dates if date >= min_date]
            else:
                return_dates=[]
                return_min_date = datetime(2200,1,1).date().strftime(MetaProcessFormat.META_DATE_FORMAT.value)        
        except s3_bucket_meta.session.client('s3').exceptions.NoSuchKey:
                return_min_date = first_date
                return_dates = [
                    (start + timedelta(days=x)).strftime(MetaProcessFormat.META_DATE_FORMAT.value) for x in range(0, (today-start).days + 1)]
        return return_min_date, return_dates


    