package scalers

import (
	"errors"
	"github.com/gocql/gocql"
	"strings"
	"testing"
)

type cassandraTestData struct {
	//test inputs
	metadata   map[string]string
	authParams map[string]string

	//expected outputs
	expectedMetricName      string
	expectedConsistency     gocql.Consistency
	expectedProtocolVersion string
	expectedError           error
}

var testCassandraInputs = []cassandraTestData{
	//metricName yazıldı
	{
		metadata:           map[string]string{"query": "SELECT COUNT(*) FROM sleep_centre.sleep_study;", "targetQueryValue": "1", "username": "cassandra", "clusterIPAddress": "my-release-cassandra.default:9042", "consistency": "Quorum", "protoVersion": "4", "metricName": "myMetric"},
		authParams:         map[string]string{"password": "enZGaEJkTlVPVA=="},
		expectedMetricName: "cassandra-myMetric",
	},

	//keyspace yazıldı
	{
		metadata:           map[string]string{"query": "SELECT COUNT(*) FROM sleep_centre.sleep_study;", "targetQueryValue": "1", "username": "cassandra", "clusterIPAddress": "my-release-cassandra.default:9042", "consistency": "Quorum", "protoVersion": "4", "keyspace": "sleep_centre"},
		authParams:         map[string]string{"password": "enZGaEJkTlVPVA=="},
		expectedMetricName: "cassandra-sleep_centre",
	},

	//metricName ve keyspace yazılmadı
	{
		metadata:           map[string]string{"query": "SELECT COUNT(*) FROM sleep_centre.sleep_study;", "targetQueryValue": "1", "username": "cassandra", "clusterIPAddress": "my-release-cassandra.default:9042", "consistency": "Quorum", "protoVersion": "4"},
		authParams:         map[string]string{"password": "enZGaEJkTlVPVA=="},
		expectedMetricName: "cassandra",
	},

	//consistency ve protoVersion yazılmadı
	{
		metadata:                map[string]string{"query": "SELECT COUNT(*) FROM sleep_centre.sleep_study;", "targetQueryValue": "1", "username": "cassandra", "clusterIPAddress": "my-release-cassandra.default:9042", "metricName": "myMetric"},
		authParams:              map[string]string{"password": "enZGaEJkTlVPVA=="},
		expectedConsistency:     gocql.One,
		expectedProtocolVersion: "4",
	},

	//Error: missing query
	{
		metadata:      map[string]string{"targetQueryValue": "1", "username": "cassandra", "clusterIPAddress": "my-release-cassandra.default:9042", "consistency": "Quorum", "protoVersion": "4", "metricName": "myMetric"},
		authParams:    map[string]string{"password": "enZGaEJkTlVPVA=="},
		expectedError: errors.New("no query given"),
	},

	//Error: missing targetQueryValue
	{
		metadata:      map[string]string{"query": "SELECT COUNT(*) FROM sleep_centre.sleep_study;", "username": "cassandra", "clusterIPAddress": "my-release-cassandra.default:9042", "consistency": "Quorum", "protoVersion": "4", "metricName": "myMetric"},
		authParams:    map[string]string{"password": "enZGaEJkTlVPVA=="},
		expectedError: errors.New("no targetQueryValue given"),
	},

	//Error: missing username
	{
		metadata:      map[string]string{"query": "SELECT COUNT(*) FROM sleep_centre.sleep_study;", "targetQueryValue": "1", "clusterIPAddress": "my-release-cassandra.default:9042", "consistency": "Quorum", "protoVersion": "4", "metricName": "myMetric"},
		authParams:    map[string]string{"password": "enZGaEJkTlVPVA=="},
		expectedError: errors.New("no username given"),
	},

	//Error: missing clusterIPAddress
	{
		metadata:      map[string]string{"query": "SELECT COUNT(*) FROM sleep_centre.sleep_study;", "targetQueryValue": "1", "username": "cassandra", "consistency": "Quorum", "protoVersion": "4", "metricName": "myMetric"},
		authParams:    map[string]string{"password": "enZGaEJkTlVPVA=="},
		expectedError: errors.New("no cluster IP address given"),
	},

	//Error: missing password
	{
		metadata:      map[string]string{"query": "SELECT COUNT(*) FROM sleep_centre.sleep_study;", "targetQueryValue": "1", "username": "cassandra", "clusterIPAddress": "my-release-cassandra.default:9042", "consistency": "Quorum", "protoVersion": "4", "metricName": "myMetric"},
		authParams:    map[string]string{},
		expectedError: errors.New("no password given"),
	},
}

func TestParseCassandraMetadata(t *testing.T) {
	for _, testData := range testCassandraInputs {
		var config = ScalerConfig{
			TriggerMetadata: testData.metadata,
			AuthParams:      testData.authParams,
		}

		outputMetadata, err := ParseCassandraMetadata(&config)
		if err != nil {
			if testData.expectedError == nil {
				t.Errorf("Unexpected error parsing input metadata: %v", err)
			} else if testData.expectedError.Error() != err.Error() {
				t.Errorf("Expected error '%v' but got '%v'", testData.expectedError, err)
			}

			continue
		}

		expectedQuery := "SELECT COUNT(*) FROM sleep_centre.sleep_study;"
		if outputMetadata.query != expectedQuery {
			t.Errorf("Wrong query. Expected '%s' but got '%s'", expectedQuery, outputMetadata.query)
		}

		expectedTargetQueryValue := 1
		if outputMetadata.targetQueryValue != expectedTargetQueryValue {
			t.Errorf("Wrong targetQueryValue. Expected %d but got %d", expectedTargetQueryValue, outputMetadata.targetQueryValue)
		}

		expectedConsistency := gocql.Quorum
		if outputMetadata.consistency != expectedConsistency {
			t.Errorf("Wrong consistency. Expected %d but got %d", expectedConsistency, outputMetadata.consistency)
		}

		expectedProtocolVersion := 4
		if outputMetadata.protoVersion != expectedProtocolVersion {
			t.Errorf("Wrong protocol version. Expected %d but got %d", expectedProtocolVersion, outputMetadata.protoVersion)
		}

		if !strings.HasPrefix(outputMetadata.metricName, "cassandra-") {
			t.Errorf("Metric name '%s' was expected to start with 'cassandra-' but got '%s'", outputMetadata.metricName, testData.expectedMetricName)
		}

		if testData.expectedMetricName != "" && testData.expectedMetricName != outputMetadata.metricName {
			t.Errorf("Wrong metric name. Expected '%s' but got '%s'", testData.expectedMetricName, outputMetadata.metricName)
		}
	}
}
