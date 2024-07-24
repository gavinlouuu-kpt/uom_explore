from flask import Flask, request, jsonify
import sqlite3

app = Flask(__name__)

# Function to get a database connection
def get_db_connection():
    conn = sqlite3.connect('voc_lab.db')
    conn.row_factory = sqlite3.Row
    return conn

# Route to fetch all measurements
@app.route('/measurements', methods=['GET'])
def get_measurements():
    conn = get_db_connection()
    measurements = conn.execute('SELECT * FROM Measurements').fetchall()
    conn.close()
    
    measurements_list = []
    for measurement in measurements:
        measurements_list.append(dict(measurement))
    
    return jsonify(measurements_list)

# Route to add a new measurement
@app.route('/measurements', methods=['POST'])
def add_measurement():
    new_measurement = request.get_json()
    
    conn = get_db_connection()
    conn.execute('''
        INSERT INTO Measurements (setup_id, channel_number, repeat_number, setting, timestamp, sensor_value, batch_id)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (
        new_measurement['setup_id'],
        new_measurement['channel_number'],
        new_measurement['repeat_number'],
        new_measurement['setting'],
        new_measurement['timestamp'],
        new_measurement['sensor_value'],
        new_measurement['batch_id']
    ))
    conn.commit()
    conn.close()
    
    return jsonify(new_measurement), 201

# Route to fetch a specific measurement by ID
@app.route('/measurements/<int:id>', methods=['GET'])
def get_measurement(id):
    conn = get_db_connection()
    measurement = conn.execute('SELECT * FROM Measurements WHERE measurement_id = ?', (id,)).fetchone()
    conn.close()
    
    if measurement is None:
        return jsonify({'error': 'Measurement not found'}), 404
    
    return jsonify(dict(measurement))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
