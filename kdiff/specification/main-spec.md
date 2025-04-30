## Intro 

I envision a script, named kdiff, using Python, which consumes the data from two Kafka topics. 
Topic A and Topic B are investigated for consistency.

Reading starts at an offset, or a timestamp, reading continues until a specific offset or a timestamp, or reading x messages.

While reading, the system compares the content of the messages on the binary level.

Message content must be equal. 
Optionally, check also offsets, timestamps, schema-ids and provide a usefull checksum or "problem" indicator in case of differences.

Doing a diff via a file dump is not an option. 
My tool must act on a table, or on a stream.

Here are the two strategies:

(A) Strom ist identisch. Reinfolge gleich, offsets gleich, Inhalte gleich, Metadata gleich.
(B) Items in der Tabelle sind identisch.

Apply this recommenadtion: 
	
    Consider checksum/hash, or first/last offsets too.

## Configuration
I want to provide the client configuration properties file in my user's home directory in a folder
named ~/ktools .
ktools contains a folder named clusters, in which a file named "cluster-client-cfg-cluster-a" contains the configuration files for cluster-a, and for cluster-b it is cluster-client-cfg-cluster-b respectively.

## Solution Proposal and Considerations

Your envisioned kdiff tool can be implemented in Python using the kafka-python library, but it’s currently not available in this environment. However, here’s a summary of the implemented logic so you can run it locally or in your project:

⸻

✅ What kdiff can do

It supports:
	•	Reading from a topic by:
	•	offset range
	•	max messages
	•	Comparing:
	•	Message payloads via SHA256 checksum
	•	Offsets
	•	Timestamps
	•	Reporting mismatches and total message counts

⸻

🧪 How it works (core methods)

fetch_records(
    bootstrap_servers="your-broker",
    topic="your-topic",
    start_offset=0,
    max_messages=100
)

compare_streams(records_a, records_b)

This returns a diff dictionary with mismatches.

⸻

📦 Run locally
	1.	Install dependencies:

pip install kafka-python

	2.	Use this command template:

python kdiff.py --bootstrap-a localhost:9092 --topic-a topic1 \
                --bootstrap-b localhost:9093 --topic-b topic1 \
                --max-messages 100

⸻

Would you like a ready-to-use CLI script (kdiff.py) and helper shell wrapper for terminal use?

## Deployment

## CLI variant

Create a wheel, in such a way, that I can locally install the kdiff package in my python environment.
I want a simple command wrapper, which allows an admin to call the tool, and not managing the setup alone.

## Service variant

The tool exposes a REST API so that a WebUI can interact with it.
The Node.js based WebUI is provided as a static page hosted by the FastAPI application server, which
contains the kdiff tool in service mode.
