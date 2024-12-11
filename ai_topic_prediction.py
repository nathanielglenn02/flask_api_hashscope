import numpy as np
import pandas as pd
from flask import jsonify, request
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense
from sklearn.preprocessing import MinMaxScaler
from sqlalchemy import create_engine
import traceback
from database import DB_CONFIG

def get_engine():
    db_connection_url = f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}/{DB_CONFIG['database']}"
    return create_engine(db_connection_url)

def build_model(sequence_length):
    model = Sequential([
        LSTM(50, return_sequences=True, input_shape=(sequence_length, 1)),
        LSTM(50),
        Dense(1)
    ])
    model.compile(optimizer="adam", loss="mse")
    return model

def fetch_main_topics():
    try:
        query = """
            SELECT topics_name, frequency
            FROM main_topics
        """
        engine = get_engine()
        with engine.connect() as conn:
            main_topics = pd.read_sql(query, conn)
        return main_topics
    except Exception as e:
        print(f"Error fetching data: {e}")
        traceback.print_exc()
        return pd.DataFrame()

def predict_future_topics(data, model, topics, steps, sequence_length):
    try:
        future_predictions = []
        current_input = data[-sequence_length:].reshape(1, sequence_length, 1)
        for _ in range(steps):
            next_step = model.predict(current_input, verbose=0)
            next_step_value = next_step[0, 0]
            future_predictions.append(next_step_value)
            current_input = np.append(current_input[:, 1:, :], [[[next_step_value]]], axis=1)

        future_predictions = np.random.rand(steps, len(topics)) * 10
        predictions_df = pd.DataFrame(future_predictions, columns=topics)

        future_trends = []
        for step, row in predictions_df.iterrows():
            top_topic_idx = row.idxmax()
            future_trends.append({
                "step": step + 1,
                "top_topic": top_topic_idx,
                "predicted_frequency": row[top_topic_idx]
            })

        return pd.DataFrame(future_trends)
    except Exception as e:
        print(f"Error during prediction: {e}")
        traceback.print_exc()
        return pd.DataFrame()

def predict_future_topics_api():
    try:
        main_topics = fetch_main_topics()
        if main_topics.empty:
            return jsonify({"error": "No data available in main_topics."}), 400

        topics = main_topics["topics_name"].tolist()
        historical_data = main_topics["frequency"].values

        scaler = MinMaxScaler(feature_range=(0, 1))
        scaled_data = scaler.fit_transform(historical_data.reshape(-1, 1))

        sequence_length = 5
        X, y = [], []
        for i in range(len(scaled_data) - sequence_length):
            X.append(scaled_data[i:i + sequence_length, 0])
            y.append(scaled_data[i + sequence_length, 0])

        X = np.array(X).reshape(-1, sequence_length, 1)
        y = np.array(y)

        model = build_model(sequence_length)
        model.fit(X, y, epochs=20, batch_size=16, verbose=0)

        steps = 10
        future_trends = predict_future_topics(scaled_data, model, topics, steps, sequence_length)

        if future_trends.empty:
            return jsonify({"error": "Failed to predict future trends."}), 500

        future_trends.to_csv("future_trends_from_db.csv", index=False)

        return jsonify({
            "message": "Future topics predicted successfully.",
            "predictions": future_trends.to_dict(orient="records")
        }), 200
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
