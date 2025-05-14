import matplotlib.pyplot as plt
import pandas as pd
import xgboost as xgb
from dagster import get_dagster_logger
from sklearn.model_selection import train_test_split


def normalize(df: pd.DataFrame) -> pd.DataFrame:
    return df / (df.abs().max())


def z_score_filter(
    df: pd.DataFrame, col_name: str, threshold: float = 3.0
) -> pd.DataFrame:
    """
    Filter outliers based on the Z-score method.
    """
    mean = df[col_name].mean()
    std = df[col_name].std()
    z_score = (df[col_name] - mean) / std
    return df[abs(z_score) < threshold]


def IQR_filter(df: pd.DataFrame, col_name: str) -> pd.DataFrame:
    """
    Filter outliers based on the Interquartile Range (IQR) method.
    """
    Q1 = df[col_name].quantile(0.5)
    Q3 = df[col_name].quantile(0.95)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    return df[(df[col_name] >= lower_bound) & (df[col_name] <= upper_bound)]


def add_lag_features(
    df: pd.DataFrame,
    group_col: str = "name",
    lags: list[int] = [1, 2, 3],
    excluded_cols: list[str] = [],
    diff: bool = True,
    mean: bool = True,
    window: int = 3,
):
    df = df[df["timeframe"] == 12].sort_index().copy()
    lag_cols = df.columns.difference([group_col] + excluded_cols)
    lagged_frames = []

    for col in lag_cols:
        for lag in lags:
            lagged = df.groupby(group_col)[col].shift(lag)
            lagged.name = f"{col}_lag{lag}"
            lagged_frames.append(lagged)

        if diff:
            df[f"{col}_diff"] = df.groupby(group_col)[col].diff()

        # Adding moving average feature (rolling mean)
        if mean:
            df[f"{col}_mean"] = (
                df.groupby(group_col)[col]
                .rolling(window=window, min_periods=1)
                .mean()
                .shift(1)
            )

    lagged_df = pd.concat(lagged_frames, axis=1)
    df = pd.concat([df, lagged_df], axis=1)
    df["target"] = df.groupby(group_col)["score"].shift(-1)
    return df


def train_list(
    df_list: list[pd.DataFrame], weight_list: list[dict]
) -> list[dict]:
    results = []
    logger = get_dagster_logger()
    for i, df in enumerate(df_list):
        logger.info(f"Training model {i + 1} of {len(df_list)}")
        logger.info(f"Weights: {weight_list[i]}")
        sf = df.dropna(subset=["target"])
        features = sf.columns.difference(["name", "target", "date", "time"])
        X = sf[features]
        y = sf["target"]

        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, shuffle=False
        )

        dtrain = xgb.DMatrix(X_train, label=y_train)
        dtest = xgb.DMatrix(X_test, label=y_test)

        evals_result: dict = {}
        params = {
            "objective": "reg:squarederror",
            "max_depth": 100,
            "learning_rate": 0.02,
            "subsample": 0.8,
            "colsample_bytree": 0.8,
            "random_state": 42,
        }

        model = xgb.train(
            params,
            dtrain,
            num_boost_round=1000,
            evals=[(dtrain, "train"), (dtest, "test")],
            evals_result=evals_result,
            early_stopping_rounds=10,
            verbose_eval=10,
        )
        results.append(
            {
                "model": model,
                "evals_result": evals_result,
                "df": df,
                "weights": weight_list[i],
            }
        )
    return results


def plot_tain_test_errors(results: list[dict]):
    _, ax = plt.subplots(len(results), 1, figsize=(10, 6 * len(results)))
    for i, result in enumerate(results):
        ax[i].plot(result["evals_result"]["train"]["rmse"], label="Train RMSE")
        ax[i].plot(result["evals_result"]["test"]["rmse"], label="Test RMSE")
        ax[i].set_xlabel("Boosting Rounds")
        ax[i].set_ylabel("RMSE")
        ax[i].set_title(
            f"Training and Test RMSE over Boosting Rounds for Model {i}"
        )
        ax[i].legend()
        ax[i].grid(True)

    plt.tight_layout()
    plt.savefig("./data/train_test_errors.png")


def plot_feature_importance(results: list[dict]):
    _, ax = plt.subplots(len(results), 1, figsize=(10, 8 * len(results)))

    for i, result in enumerate(results):
        model = result["model"]
        plt.title(f"Feature Importance for Model {i}")
        xgb.plot_importance(
            model,
            ax=ax[i],
            title=f"Feature Importance for Model {i}",
            max_num_features=20,
        )
    plt.tight_layout()
    plt.savefig("./data/feature_importance.png")


def predict(results: list[dict]) -> pd.DataFrame:
    logger = get_dagster_logger()
    idx = sorted(
        [
            (i, result["model"].best_score / result["df"]["score"].std())
            for i, result in enumerate(results)
        ],
        key=lambda x: x[1],
    )[0][0]
    logger.info(
        "Best model was selected "
        f"with Score definition: {results[idx]['weights']}"
    )
    best_model: xgb.Booster = results[idx]["model"]
    df: pd.DataFrame = results[idx]["df"]
    features = df[df["target"].isna()].drop(
        columns=["name", "target", "time"]
    )[best_model.feature_names]
    predictions = best_model.predict(xgb.DMatrix(features))
    logger.info(f"Prediction count: {len(predictions)}")
    df.loc[df["target"].isna(), "prediction"] = predictions
    return df.dropna(subset=["prediction"])[
        ["name", "industry_group", "prediction"]
    ].rename(columns={"prediction": "score"})
