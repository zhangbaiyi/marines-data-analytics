import pickle
from typing import Dict, List

import numpy as np
import pandas as pd
import spacy_universal_sentence_encoder
from langcodes import Language
from sklearn.linear_model import LogisticRegression

from src.scripts.utils import resolve_import_path_from_project_root


class SentimentAnalysis:
    def __init__(self, file_path: str):
        self._SPACY_USE_NLP_MODEL = spacy_universal_sentence_encoder.load_model(
            "en_use_md"
        )
        if self._SPACY_USE_NLP_MODEL is None:
            raise ValueError(
                'Error: Unable to load the "Spacy Universal Sentence Encoder" Model'
            )

        self._df_customer_survey_responses = self._preprocess_excel_data(file_path)

    def _print_excel_workbook_metadata(
        self, excel_workbook: Dict[str, pd.DataFrame]
    ) -> None:
        for sheet_name, df_sheet in excel_workbook.items():
            print(f"Displaying Sheet: {sheet_name}")

            num_rows_to_preview_display = 10
            if sheet_name == "Metadata":
                num_rows_to_preview_display = 20

            print(df_sheet.head(n=num_rows_to_preview_display))
            print(df_sheet.info())

            if "answerFreeTextValues" in set(df_sheet.columns):
                print(df_sheet["answerFreeTextValues"].notna())
                print(df_sheet["answerFreeTextValues"].notna().value_counts())

            print("\n\n\n")

    def _generate_single_combined_dataframe(
        self,
        expected_metadata_columns: List[str],
        excel_workbook: Dict[str, pd.DataFrame],
    ) -> pd.DataFrame:
        df_customer_survey_responses = pd.DataFrame(
            columns=["sheetName"] + expected_metadata_columns
        )

        for sheet_name, df_sheet in excel_workbook.items():
            print(f"Processing Sheet: {sheet_name}")

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
        print(f"Metadata Columns: {expected_metadata_columns}")
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

        print(df_customer_survey_responses.head())

        print(df_customer_survey_responses.info())
        print(
            df_customer_survey_responses["answerFreeTextValues"].notna().value_counts()
        )

        df_customer_survey_responses_filtered = df_customer_survey_responses.dropna(
            axis=1
        )
        print(
            f"[After Dropping of NaN Filtering] Columns Left: {set(df_customer_survey_responses_filtered.columns)}"
        )
        print(df_customer_survey_responses_filtered.info())
        print(
            df_customer_survey_responses_filtered["answerFreeTextValues"]
            .notna()
            .value_counts()
        )

        df_customer_survey_responses_filtered = df_customer_survey_responses_filtered[
            ~df_customer_survey_responses_filtered["questionId"].str.startswith(
                "CPP", na=False
            )
        ]
        print(
            f'[After "CPP" Filtering] Columns Left: {set(df_customer_survey_responses_filtered.columns)}'
        )
        print(df_customer_survey_responses_filtered.info())
        print(
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

    def predict_sentiment_analysis(self) -> Dict[str, float]:
        sentence_embeddings_for_prediction = self._generate_sentence_embeddings()

        result_dict: Dict[str, float] = {}
        with open(
            resolve_import_path_from_project_root(
                "src/models/ml_sentiment_analysis_model.pkl"
            ),
            "rb",
        ) as ml_model_file, open(
            resolve_import_path_from_project_root(
                "src/models/ml_sentiment_analysis_label_encoder_mapping_results.pkl"
            ),
            "rb",
        ) as label_encoder_mapping_file:

            label_encoder_mapping: Dict[int, str] = pickle.load(
                label_encoder_mapping_file
            )

            ml_model: LogisticRegression = pickle.load(ml_model_file)
            y_pred: np.ndarray = ml_model.predict(X=sentence_embeddings_for_prediction)
            print(f"Predicted Labels: {y_pred}")

            total_predictions = len(y_pred)
            print(f"Total Predictions: {total_predictions}")

            label_counts: Dict[int, float] = {
                label: np.sum(y_pred == label) for label in np.unique(y_pred)
            }
            print(f"Label Counts: {label_counts}")

            for label, count in label_counts.items():
                sentiment_label = label_encoder_mapping.get(
                    label
                )  # Get the label from the mapping
                result_dict[sentiment_label] = count
            print(f"Result Dictionary (After Mapping): {result_dict}")

            for sentiment_label, count in result_dict.items():
                percentage = (count / total_predictions) * 100
                result_dict[sentiment_label] = percentage

        print(f"Result Dictionary (After Percentage Calculation): {result_dict}")
        return result_dict
