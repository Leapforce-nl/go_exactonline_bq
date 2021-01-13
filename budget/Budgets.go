package exactonline_bq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"

	errortools "github.com/leapforce-libraries/go_errortools"
	budget "github.com/leapforce-libraries/go_exactonline_new/budget"
	google "github.com/leapforce-libraries/go_google"
	types "github.com/leapforce-libraries/go_types"
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
		google.DateToNullTimestamp(c.Created),
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
		google.DateToNullTimestamp(c.Modified),
		c.Modifier.String(),
		c.ModifierFullName,
		c.ReportingPeriod,
		c.ReportingYear,
	}
}

func (service *Service) WriteBudgetsBQ(bucketHandle *storage.BucketHandle, lastModified *time.Time) ([]*storage.ObjectHandle, int, interface{}, *errortools.Error) {
	if bucketHandle == nil {
		return nil, 0, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	call := service.BudgetService().NewGetBudgetsCall(lastModified)

	rowCount := 0
	batchRowCount := 0
	batchSize := 10000

	for true {
		budgets, e := call.Do()
		if e != nil {
			return nil, 0, nil, e
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

			b, err := json.Marshal(getBudgetBQ(&tl, service.ClientID()))
			if err != nil {
				return nil, 0, nil, errortools.ErrorMessage(err)
			}

			// Write data
			_, err = w.Write(b)
			if err != nil {
				return nil, 0, nil, errortools.ErrorMessage(err)
			}

			// Write NewLine
			_, err = fmt.Fprintf(w, "\n")
			if err != nil {
				return nil, 0, nil, errortools.ErrorMessage(err)
			}
		}

		if batchRowCount > batchSize {
			// Close and flush data
			err := w.Close()
			if err != nil {
				return nil, 0, nil, errortools.ErrorMessage(err)
			}
			w = nil

			fmt.Printf("#Budgets for service %s flushed: %v\n", service.ClientID(), batchRowCount)

			rowCount += batchRowCount
			batchRowCount = 0
		}
	}

	if w != nil {
		// Close and flush data
		err := w.Close()
		if err != nil {
			return nil, 0, nil, errortools.ErrorMessage(err)
		}

		rowCount += batchRowCount
	}

	fmt.Printf("#Budgets for service %s: %v\n", service.ClientID(), rowCount)

	return objectHandles, rowCount, BudgetBQ{}, nil
}
