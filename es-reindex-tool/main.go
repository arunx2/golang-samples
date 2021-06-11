package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/namsral/flag"
)

var (
	aliasName             string
	elasticUrl            string
	userName              string
	password              string
	indexTemplateLocation string
	indexTemplateName     string
	xtraAlias             string
	Es                    *elasticsearch.Client
)

func init() {
	flag.StringVar(&aliasName, "alias", "", "alias name")
	flag.StringVar(&xtraAlias, "extra-alias", "", "Additional alias name to update with new index")
	flag.StringVar(&elasticUrl, "elastic-url", "", "elastic url")
	flag.StringVar(&userName, "username", "", "elastic user name")
	flag.StringVar(&password, "password", "", "elastic password")
	flag.StringVar(&indexTemplateName, "template-name", "", "Template mapping to the index")
	flag.StringVar(&indexTemplateLocation, "template-location", "", "Absolute file path to the index template mapping")

	flag.Parse()
	if aliasName == "" {
		//alias name is a mandatory input
		panic("alias name is missing!")
	}
	cfg := elasticsearch.Config{
		Addresses: []string{
			elasticUrl,
		},
	}
	if userName != "" && password != "" {
		cfg.Username = userName
		cfg.Password = password
	}
	Es, _ = elasticsearch.NewClient(cfg)
}

func main() {
	//publish new mapping template
	bytes, _ := ioutil.ReadFile(indexTemplateLocation)
	if len(bytes) > 0 {
		request := esapi.IndicesPutIndexTemplateRequest{
			Body:   strings.NewReader(string(bytes)),
			Name:   indexTemplateName,
			Pretty: true,
		}
		_, err := request.Do(context.Background(), Es)
		if err != nil {
			return
		}
	}
	//get the index pointed by alias
	request := esapi.CatAliasesRequest{
		Name: []string{aliasName},
	}
	aliasResponse, err := request.Do(context.Background(), Es)
	if err == nil {
		//continue
		if aliasResponse.StatusCode == http.StatusOK {
			responseBody, _ := ioutil.ReadAll(aliasResponse.Body)
			currentIndexName := getCurrentIndexName(string(responseBody))

			newIndexName := getNewIndexName(currentIndexName)
			//reindex from current index to new index
			err := reindex(currentIndexName, newIndexName)
			if err == nil {
				//replace the new index to alias
				err := switchAlias(aliasName, currentIndexName, newIndexName)
				if err == nil {
					_ = closeIndex(currentIndexName)
					fmt.Printf("new index %s created and %s pointing to %s", newIndexName, aliasName, newIndexName)
				}
			}

		}
		fmt.Printf("%v", aliasResponse)
	} else {
		fmt.Print(err)
	}

}

func closeIndex(indexName string) (err error) {
	closeIndexRequest := esapi.IndicesCloseRequest{
		Index: []string{indexName},
	}
	_, err = closeIndexRequest.Do(context.Background(), Es)
	return err
}

func switchAlias(aliasName string, currentIndexName string, newIndexName string) (err error) {
	updateAliasQuery := `{
  			"actions": [
    		{
      			"add": {
        		"index": "` + newIndexName + `",
        		"alias": "` + aliasName + `"
      		}	
    		},{
      			"remove": {
         			"index": "` + currentIndexName + `",
        			"alias": "` + aliasName + `"
      			}
    		}`
	if xtraAlias != "" {
		updateAliasQuery = updateAliasQuery + `,{
      			"add": {
        		"index": "` + newIndexName + `",
        		"alias": "` + xtraAlias + `"
      		}	
    		},{
      			"remove": {
         			"index": "` + currentIndexName + `",
        			"alias": "` + xtraAlias + `"
      			}
    		}`
	}
	updateAliasQuery = updateAliasQuery + `]}`
	aliasRequest := esapi.IndicesUpdateAliasesRequest{
		Body: strings.NewReader(updateAliasQuery),
	}

	_, err = aliasRequest.Do(context.Background(), Es)
	return
}

func reindex(currentIndexName string, newIndexName string) (err error) {
	reindexQuery := `{
  		"source": {
   			 "index": "` + currentIndexName + `"
  		},
		"dest": {
    		"index": "` + newIndexName + `"
  		}
	}`
	waitForCompletion := true
	reindexRequest := esapi.ReindexRequest{
		Body:              strings.NewReader(reindexQuery),
		WaitForCompletion: &waitForCompletion,
		Pretty:            false,
	}
	_, err = reindexRequest.Do(context.Background(), Es)
	return
}

func getNewIndexName(currentIndexName string) (indexName string) {
	lastIndex := strings.LastIndex(currentIndexName, "_")
	currentTime := time.Now()
	indexSuffix := currentTime.Format("2006-01-02-15-04")
	var indexPrefix string
	if lastIndex > 0 {
		//runes := []rune(currentIndexName)
		indexPrefix = currentIndexName[:lastIndex]
	} else {
		indexPrefix = currentIndexName
	}
	indexName = indexPrefix + "_" + indexSuffix
	return
}

func getCurrentIndexName(aliasResponse string) (indexName string) {
	split := strings.Split(aliasResponse, " ")
	//0th element is alias name 1st element is index name
	if len(split) > 2 {
		indexName = split[1]
	}
	return
}
