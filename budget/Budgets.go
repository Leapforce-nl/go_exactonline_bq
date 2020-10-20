package exactonline_bq

import (
	"context"
	"encoding/json"
	"fmt"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"

	bigquerytools "github.com/Leapforce-nl/go_bigquerytools"
	budget "github.com/Leapforce-nl/go_exactonline_new/budget"
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

func (client *Client) GetBudgetsBQ() (*[]BudgetBQ, error) {
	gds, err := client.ExactOnline().BudgetClient.GetBudgets()
	if err != nil {
		return nil, err
	}

	if gds == nil {
		return nil, nil
	}

	rowCount := len(*gds)

	fmt.Printf("#Budgets for client %s: %v\n", client.ClientID(), rowCount)

	gdsBQ := []BudgetBQ{}

	for _, gd := range *gds {
		gdsBQ = append(gdsBQ, getBudgetBQ(&gd, client.ClientID()))
	}

	return &gdsBQ, nil
}

func (client *Client) WriteBudgetsBQ(writeToObject *storage.ObjectHandle) (interface{}, error) {
	if writeToObject == nil {
		return nil, nil
	}

	gdsBQ, err := client.GetBudgetsBQ()
	if err != nil {
		return nil, err
	}

	if gdsBQ == nil {
		return nil, nil
	}

	ctx := context.Background()

	w := writeToObject.NewWriter(ctx)

	for _, gdBQ := range *gdsBQ {

		b, err := json.Marshal(gdBQ)
		if err != nil {
			return nil, err
		}

		// Write data
		_, err = w.Write(b)
		if err != nil {
			return nil, err
		}

		// Write NewLine
		_, err = fmt.Fprintf(w, "\n")
		if err != nil {
			return nil, err
		}
	}

	// Close
	err = w.Close()
	if err != nil {
		return nil, err
	}

	return BudgetBQ{}, nil
}
