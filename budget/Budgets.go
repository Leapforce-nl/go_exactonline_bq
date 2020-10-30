package exactonline_bq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"

	bigquerytools "github.com/Leapforce-nl/go_bigquerytools"
	budget "github.com/Leapforce-nl/go_exactonline_new/budget"
	types "github.com/Leapforce-nl/go_types"
)

type BudgetBQ struct {
	ClientID                  string
	ID                        string
	AmountDC                  float64
	BudgetScenario            string
	BudgetScenarioCode        string
	BudgetScenarioDescription string
	Costcenter                string
	CostcenterDescription     string
	Costunit                  string
	CostunitDescription       string
	Created                   bigquery.NullTimestamp
	Creator                   string
	CreatorFullName           string
	Division                  int32
	GLAccount                 string
	GLAccountCode             string
	GLAccountDescription      string
	HID                       string
	Item                      string
	ItemCode                  string
	ItemDescription           string
	Modified                  bigquery.NullTimestamp
	Modifier                  string
	ModifierFullName          string
	ReportingPeriod           int16
	ReportingYear             int16
}

func getBudgetBQ(c *budget.Budget, clientID string) BudgetBQ {
	return BudgetBQ{
		clientID,
		c.ID.String(),
		c.AmountDC,
		c.BudgetScenario.String(),
		c.BudgetScenarioCode,
		c.BudgetScenarioDescription,
		c.Costcenter,
		c.CostcenterDescription,
		c.Costunit,
		c.CostunitDescription,
		bigquerytools.DateToNullTimestamp(c.Created),
		c.Creator.String(),
		c.CreatorFullName,
		c.Division,
		c.GLAccount.String(),
		c.GLAccountCode,
		c.GLAccountDescription,
		c.HID,
		c.Item.String(),
		c.ItemCode,
		c.ItemDescription,
		bigquerytools.DateToNullTimestamp(c.Modified),
		c.Modifier.String(),
		c.ModifierFullName,
		c.ReportingPeriod,
		c.ReportingYear,
	}
}

func (client *Client) WriteBudgetsBQ(bucketHandle *storage.BucketHandle, lastModified *time.Time) ([]*storage.ObjectHandle, int, interface{}, error) {
	if bucketHandle == nil {
		return nil, 0, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	call := client.ExactOnline().BudgetClient.NewGetBudgetsCall(lastModified)

	rowCount := 0
	batchRowCount := 0
	batchSize := 10000

	for true {
		budgets, err := call.Do()
		if err != nil {
			return nil, 0, nil, err
		}

		if budgets == nil {
			break
		}

		if batchRowCount == 0 {
			guid := types.NewGUID()
			objectHandle := bucketHandle.Object((&guid).String())
			objectHandles = append(objectHandles, objectHandle)

			w = objectHandle.NewWriter(context.Background())
		}

		for _, tl := range *budgets {
			batchRowCount++

			b, err := json.Marshal(getBudgetBQ(&tl, client.ClientID()))
			if err != nil {
				return nil, 0, nil, err
			}

			// Write data
			_, err = w.Write(b)
			if err != nil {
				return nil, 0, nil, err
			}

			// Write NewLine
			_, err = fmt.Fprintf(w, "\n")
			if err != nil {
				return nil, 0, nil, err
			}
		}

		if batchRowCount > batchSize {
			// Close and flush data
			err = w.Close()
			if err != nil {
				return nil, 0, nil, err
			}
			w = nil

			fmt.Printf("#Budgets for client %s flushed: %v\n", client.ClientID(), batchRowCount)

			rowCount += batchRowCount
			batchRowCount = 0
		}
	}

	if w != nil {
		// Close and flush data
		err := w.Close()
		if err != nil {
			return nil, 0, nil, err
		}

		rowCount += batchRowCount
	}

	fmt.Printf("#Budgets for client %s: %v\n", client.ClientID(), rowCount)

	return objectHandles, rowCount, BudgetBQ{}, nil
}
