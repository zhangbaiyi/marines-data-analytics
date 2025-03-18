import pickle
from typing import Dict, List, Tuple, cast

import numpy as np
import pandas as pd
import spacy_universal_sentence_encoder
from langcodes import Language
from sklearn.linear_model import LogisticRegression

from src.scripts.utils import generate_curr_date_to_append_to_filename, resolve_import_path_from_project_root
from src.utils.logging import LOGGER


class SentimentAnalysis:
    def __init__(
        self, rel_file_path: str = "../data-lake/CustomerSurveyResponses.xlsx"
    ):
        self._SPACY_USE_NLP_MODEL = spacy_universal_sentence_encoder.load_model(
            "en_use_md"
        )
        if self._SPACY_USE_NLP_MODEL is None:
            raise ValueError(
                'Error: Unable to load the "Spacy Universal Sentence Encoder" Model'
            )

        self._df_customer_survey_responses = self._preprocess_excel_data(
            rel_file_path)

    def _print_excel_workbook_metadata(
        self, excel_workbook: Dict[str, pd.DataFrame]
    ) -> None:
        for sheet_name, df_sheet in excel_workbook.items():
            LOGGER.debug(f"Displaying Sheet: {sheet_name}")

            num_rows_to_preview_display = 10
            if sheet_name == "Metadata":
                num_rows_to_preview_display = 20

            LOGGER.debug(df_sheet.head(n=num_rows_to_preview_display))
            LOGGER.debug(df_sheet.info())

            if "answerFreeTextValues" in set(df_sheet.columns):
                LOGGER.debug(df_sheet["answerFreeTextValues"].notna())
                LOGGER.debug(
                    df_sheet["answerFreeTextValues"].notna().value_counts())

            LOGGER.debug("\n\n\n")

    def _generate_single_combined_dataframe(
        self,
        expected_metadata_columns: List[str],
        excel_workbook: Dict[str, pd.DataFrame],
    ) -> pd.DataFrame:
        df_customer_survey_responses = pd.DataFrame(
            columns=["sheetName"] + expected_metadata_columns
        )

        for sheet_name, df_sheet in excel_workbook.items():
            LOGGER.debug(f"Processing Sheet: {sheet_name}")

            if sheet_name == "Metadata":
                continue

            df_sheet = df_sheet.reindex(columns=expected_metadata_columns)
            df_sheet.dropna(subset=["answerFreeTextValues"], inplace=True)

            df_sheet["sheetName"] = sheet_name

            df_customer_survey_responses = pd.concat(
                [df_customer_survey_responses, df_sheet], ignore_index=True
            )

        return df_customer_survey_responses

    def _preprocess_excel_data(self, rel_file_path: str) -> pd.DataFrame:
        excel_workbook: Dict[str, pd.DataFrame] = pd.read_excel(
            resolve_import_path_from_project_root(rel_file_path), sheet_name=None
        )
        self._print_excel_workbook_metadata(excel_workbook)

        assert (
            len(excel_workbook) != 0
        ), "Error: Expected a non-zero count of sheets to be found in the Excel Workbook"

        # Assuming the expected column metadata is the same across all sheets (and not typically denoted on the first sheet)
        expected_metadata_columns = (
            list(list(excel_workbook.values())[1].columns)
            if len(excel_workbook) > 1
            else list(list(excel_workbook.values())[0].columns)
        )
        LOGGER.debug(f"Metadata Columns: {expected_metadata_columns}")
        df_customer_survey_responses = list(excel_workbook.values())[0]
        if "sheetName" not in expected_metadata_columns:
            df_customer_survey_responses = self._generate_single_combined_dataframe(
                expected_metadata_columns, excel_workbook
            )
            assert (
                "sheetName" in df_customer_survey_responses.columns
            ), 'Error: Expected "sheetName" column not found in the Metadata Columns (even after combining into a single Dataframe)'

        assert (
            "answerFreeTextValues" in expected_metadata_columns
        ), 'Error: Expected "answerFreeTextValues" column not found in the Metadata Columns'

        LOGGER.debug(df_customer_survey_responses.head())

        LOGGER.debug(df_customer_survey_responses.info())
        LOGGER.debug(
            df_customer_survey_responses["answerFreeTextValues"].notna(
            ).value_counts()
        )

        df_customer_survey_responses_filtered = df_customer_survey_responses.dropna(
            axis=1
        )
        LOGGER.debug(
            f"[After Dropping of NaN Filtering] Columns Left: {set(df_customer_survey_responses_filtered.columns)}"
        )
        LOGGER.debug(df_customer_survey_responses_filtered.info())
        LOGGER.debug(
            df_customer_survey_responses_filtered["answerFreeTextValues"]
            .notna()
            .value_counts()
        )

        df_customer_survey_responses_filtered = df_customer_survey_responses_filtered[
            ~df_customer_survey_responses_filtered["questionId"].str.startswith(
                "CPP", na=False
            )
        ]
        LOGGER.debug(
            f'[After "CPP" Filtering] Columns Left: {set(df_customer_survey_responses_filtered.columns)}'
        )
        LOGGER.debug(df_customer_survey_responses_filtered.info())
        LOGGER.debug(
            df_customer_survey_responses_filtered["answerFreeTextValues"]
            .notna()
            .value_counts()
        )

        return df_customer_survey_responses_filtered

    def _generate_sentence_embeddings(self) -> np.ndarray:
        if self._df_customer_survey_responses.empty:
            raise ValueError(
                'Error: Expected "Customer Survey Results" Dataframe should not be empty'
            )

        X_SheetName_series: pd.Series = self._df_customer_survey_responses["sheetName"]
        sheetname_docs: List[Language] = []
        for idx, item in X_SheetName_series.items():
            sheetname_docs.append(self._SPACY_USE_NLP_MODEL(item))
        X_SheetName_use: List[np.ndarray] = list(
            map(lambda doc: doc.vector, sheetname_docs)
        )

        X_QuestionName_series: pd.Series = self._df_customer_survey_responses[
            "questionName"
        ]
        questionname_docs: List[Language] = []
        for idx, item in X_QuestionName_series.items():
            questionname_docs.append(self._SPACY_USE_NLP_MODEL(item))
        X_QuestionName_use: List[np.ndarray] = list(
            map(lambda doc: doc.vector, questionname_docs)
        )

        X_QuestionType_series: pd.Series = self._df_customer_survey_responses[
            "questionType"
        ]
        questiontype_docs: List[Language] = []
        for idx, item in X_QuestionType_series.items():
            questiontype_docs.append(self._SPACY_USE_NLP_MODEL(item))
        X_QuestionType_use: List[np.ndarray] = list(
            map(lambda doc: doc.vector, questiontype_docs)
        )

        X_QuestionLabel_series: pd.Series = self._df_customer_survey_responses[
            "questionLabel"
        ]
        questionlabel_docs: List[Language] = []
        for idx, item in X_QuestionLabel_series.items():
            questionlabel_docs.append(self._SPACY_USE_NLP_MODEL(item))
        X_QuestionLabel_use: List[np.ndarray] = list(
            map(lambda doc: doc.vector, questionlabel_docs)
        )

        X_AnswerFreeTextValues_series: pd.Series = self._df_customer_survey_responses[
            "answerFreeTextValues"
        ]
        answerfreetextvalues_docs: List[Language] = []
        for idx, item in X_AnswerFreeTextValues_series.items():
            answerfreetextvalues_docs.append(self._SPACY_USE_NLP_MODEL(item))
        X_AnswerFreeTextValues_use: List[np.ndarray] = list(
            map(lambda doc: doc.vector, answerfreetextvalues_docs)
        )

        combined_sentence_embeddings = np.hstack(
            (
                X_SheetName_use,
                X_QuestionName_use,
                X_QuestionType_use,
                X_QuestionLabel_use,
                X_AnswerFreeTextValues_use,
            )
        )
        return combined_sentence_embeddings

    def predict_sentiment_analysis(self) -> Tuple[pd.DataFrame, Dict[str, float]]:
        assert (self._df_customer_survey_responses["sentiment"] == "").all(
        ), "Error: Expected all \"sentiment\" values to be empty before attempting to predict sentiment analysis"

        date_timestamp: str = generate_curr_date_to_append_to_filename()
        LOGGER.debug(f"Current Date Timestamp: {date_timestamp}")
        sentence_embeddings_for_prediction = self._generate_sentence_embeddings()

        result_dict: Dict[str, float] = {}
        with open(
            resolve_import_path_from_project_root(
                f"src/models/ml_sentiment_analysis_model_{date_timestamp}.pkl"
            ),
            "rb",
        ) as ml_model_file, open(
            resolve_import_path_from_project_root(
                f"src/models/ml_sentiment_analysis_label_encoder_mapping_results_{date_timestamp}.pkl"
            ),
            "rb",
        ) as label_encoder_mapping_file:

            label_encoder_mapping: Dict[int, str] = pickle.load(
                label_encoder_mapping_file
            )

            ml_model: LogisticRegression = pickle.load(ml_model_file)
            y_pred: np.ndarray = ml_model.predict(
                X=sentence_embeddings_for_prediction)
            LOGGER.debug(f"Predicted Labels: {y_pred}")

            self._df_customer_survey_responses.drop(
                labels=["sentiment"], axis=1, inplace=True)
            # Label Encoder Mapping: {0: 'B', 1: 'G', 2: 'N'}
            assert y_pred.ndim == 1, "Error: Expected 1-Dimensional Array for Prediction Results"
            sentiment_pred_results_pandas_series: pd.Series = pd.Series(map(
                lambda pred_entry: label_encoder_mapping.get(int(cast(str, pred_entry))), y_pred), name="sentiment")
            self._df_customer_survey_responses = pd.concat(
                [self._df_customer_survey_responses, sentiment_pred_results_pandas_series], axis=1)
            LOGGER.debug(self._df_customer_survey_responses.head())

            total_predictions = len(y_pred)
            LOGGER.debug(f"Total Predictions: {total_predictions}")

            label_counts: Dict[int, float] = {
                label: np.sum(y_pred == label) for label in np.unique(y_pred)
            }
            LOGGER.debug(f"Label Counts: {label_counts}")

            for label, count in label_counts.items():
                sentiment_label = label_encoder_mapping.get(
                    label
                )  # Get the label from the mapping
                result_dict[sentiment_label] = count
            LOGGER.debug(f"Result Dictionary (After Mapping): {result_dict}")

            for sentiment_label, count in result_dict.items():
                percentage = (count / total_predictions) * 100
                result_dict[sentiment_label] = percentage

        LOGGER.debug(
            f"Result Dictionary (After Percentage Calculation): {result_dict}")
        return self._df_customer_survey_responses, result_dict
