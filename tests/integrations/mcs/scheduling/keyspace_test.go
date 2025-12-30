package scheduling

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/client/testutil"
	"github.com/tikv/pd/pkg/schedule/labeler"
	"github.com/tikv/pd/server/apiv2/handlers"
	"github.com/tikv/pd/tests"
)

func TestKeyspaceRegionLabeler(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var cluster *tests.TestCluster
	var pdLeader *tests.TestServer
	var err error
	var rule *labeler.LabelRule
	checkRuleFn := func(empty bool) {
		cluster, err = tests.NewTestAPICluster(ctx, 1)
		re.NoError(err)

		err = cluster.RunInitialServers()
		re.NoError(err)
		leaderName := cluster.WaitLeader()
		re.NotEmpty(leaderName)

		pdLeader = cluster.GetServer(leaderName)
		re.NoError(pdLeader.BootstrapCluster())

		testutil.Eventually(re, func() bool {
			rc := pdLeader.GetRaftCluster()
			if rc == nil {
				return false
			}
			return true
		})
		rc := pdLeader.GetRaftCluster()
		re.NotNil(rc)
		labler := rc.GetRegionLabeler()
		rules := labler.GetAllLabelRules()
		if empty {
			re.Empty(rules)
		} else {
			re.NotEmpty(rules)
			rule = rules[0]
			cluster.Destroy()
		}
		fmt.Printf("init rules: %+v\n", rules)
	}
	checkRuleFn(false)

	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/schedule/labeler/skipLoadRules", "return"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/schedule/labeler/skipLoadRules"))
	}()
	checkRuleFn(true)

	checkRuleApi := func() {
		address := pdLeader.GetAddr()
		labelPrefix := "/pd/api/v1/config/region-label/rule"

		// get rule
		resp, err := http.DefaultClient.Get(address + labelPrefix + "/0")
		re.NoError(err)
		re.Equal(http.StatusNotFound, resp.StatusCode)
		re.NoError(resp.Body.Close())

		// batch get rules
		req, err := http.NewRequest(http.MethodGet, address+"/pd/api/v1/config/region-label/rules/ids", bytes.NewBuffer([]byte(`["rule1", "rule3"]`)))
		re.NoError(err)
		resp, err = http.DefaultClient.Do(req)
		re.NoError(err)
		re.Equal(http.StatusInternalServerError, resp.StatusCode)
		re.NoError(resp.Body.Close())

		// delete rule
		req, err = http.NewRequest(http.MethodDelete, address+labelPrefix+"/0", nil)
		re.NoError(err)
		resp, err = http.DefaultClient.Do(req)
		re.NoError(err)
		re.Equal(http.StatusInternalServerError, resp.StatusCode)
		re.NoError(resp.Body.Close())

		// update
		rule.Index = 1
		data, _ := json.Marshal(rule)
		resp, err = http.DefaultClient.Post(address+labelPrefix, "application/json", bytes.NewBuffer(data))
		re.NoError(err)
		re.Equal(http.StatusInternalServerError, resp.StatusCode)
		re.NoError(resp.Body.Close())

		// patch
		patch := labeler.LabelRulePatch{
			SetRules: []*labeler.LabelRule{
				{ID: "0", Labels: []labeler.RegionLabel{{Key: "k2", Value: "v2"}}, RuleType: "key-range", Data: map[string]string{"start_key": "", "end_key": ""}},
			},
			DeleteRules: []string{"rule1"},
		}
		data, _ = json.Marshal(patch)

		// patch rules
		req, err = http.NewRequest(http.MethodPatch, address+labelPrefix+"s", bytes.NewBuffer(data))
		re.NoError(err)
		resp, err = http.DefaultClient.Do(req)
		re.NoError(err)
		re.Equal(http.StatusInternalServerError, resp.StatusCode)
		re.NoError(resp.Body.Close())

	}
	checkRuleApi()

	// create keyspace
	testConfig := map[string]string{
		"config1": "100",
		"config2": "200",
	}
	createRequest := &handlers.CreateKeyspaceParams{
		Name:   "test_keyspace",
		Config: testConfig,
	}

	address := pdLeader.GetAddr()
	data, _ := json.Marshal(createRequest)
	resp, err := http.DefaultClient.Post(address+"/pd/api/v2/keyspaces", "application/json", bytes.NewBuffer(data))
	re.NoError(err)
	re.Equal(http.StatusInternalServerError, resp.StatusCode)
	re.NoError(resp.Body.Close())
	re.True(false)
}
