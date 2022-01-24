package exactonline_bq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	_bigquery "cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"

	errortools "github.com/leapforce-libraries/go_errortools"
	budget "github.com/leapforce-libraries/go_exactonline_new/budget"
	bigquery "github.com/leapforce-libraries/go_google/bigquery"
	types "github.com/leapforce-libraries/go_types"
)

type Budget struct {
	OrganisationID_           int64
	SoftwareClientLicenceID_  int64
	Created_                  time.Time
	Modified_                 time.Time
	ID                        string
	AmountDC                  float64
	BudgetScenario            string
	BudgetScenarioCode        string
	BudgetScenarioDescription string
	Costcenter                string
	CostcenterDescription     string
	Costunit                  string
	CostunitDescription       string
	Created                   _bigquery.NullTimestamp
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
	Modified                  _bigquery.NullTimestamp
	Modifier                  string
	ModifierFullName          string
	ReportingPeriod           int16
	ReportingYear             int16
}

func getBudget(c *budget.Budget, organisationID int64, softwareClientLicenceID int64) Budget {
	t := time.Now()

	return Budget{
		organisationID,
		softwareClientLicenceID,
		t, t,
		c.ID.String(),
		c.AmountDC,
		c.BudgetScenario.String(),
		c.BudgetScenarioCode,
		c.BudgetScenarioDescription,
		c.Costcenter,
		c.CostcenterDescription,
		c.Costunit,
		c.CostunitDescription,
		bigquery.DateToNullTimestamp(c.Created),
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
		bigquery.DateToNullTimestamp(c.Modified),
		c.Modifier.String(),
		c.ModifierFullName,
		c.ReportingPeriod,
		c.ReportingYear,
	}
}

func (service *Service) WriteBudgetsBQ(bucketHandle *storage.BucketHandle, organisationID int64, softwareClientLicenceID int64, lastModified *time.Time) ([]*storage.ObjectHandle, int, interface{}, *errortools.Error) {
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

			b, err := json.Marshal(getBudget(&tl, organisationID, softwareClientLicenceID))
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

			fmt.Printf("#Budgets for flushed: %v\n", batchRowCount)

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

	fmt.Printf("#Budgets: %v\n", rowCount)

	return objectHandles, rowCount, Budget{}, nil
}
