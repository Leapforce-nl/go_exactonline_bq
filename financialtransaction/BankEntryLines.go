package exactonline_bq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"

	bigquerytools "github.com/Leapforce-nl/go_bigquerytools"
	financialtransaction "github.com/Leapforce-nl/go_exactonline_new/financialtransaction"
)

type BankEntryLineBQ struct {
	ClientID              string
	ID                    string
	Account               string
	AccountCode           string
	AccountName           string
	AmountDC              float64
	AmountFC              float64
	AmountVATFC           float64
	Asset                 string
	AssetCode             string
	AssetDescription      string
	CostCenter            string
	CostCenterDescription string
	CostUnit              string
	CostUnitDescription   string
	Created               bigquery.NullTimestamp
	Creator               string
	CreatorFullName       string
	Date                  bigquery.NullTimestamp
	Description           string
	Division              int32
	Document              string
	DocumentNumber        int32
	DocumentSubject       string
	EntryID               string
	EntryNumber           int32
	ExchangeRate          float64
	GLAccount             string
	GLAccountCode         string
	GLAccountDescription  string
	LineNumber            int32
	Modified              bigquery.NullTimestamp
	Modifier              string
	ModifierFullName      string
	Notes                 string
	OffsetID              string
	OurRef                int32
	Project               string
	ProjectCode           string
	ProjectDescription    string
	Quantity              float64
	VATCode               string
	VATCodeDescription    string
	VATPercentage         float64
	VATType               string
}

func getBankEntryLineBQ(c *financialtransaction.BankEntryLine, clientID string) BankEntryLineBQ {
	return BankEntryLineBQ{
		clientID,
		c.ID.String(),
		c.Account.String(),
		c.AccountCode,
		c.AccountName,
		c.AmountDC,
		c.AmountFC,
		c.AmountVATFC,
		c.Asset.String(),
		c.AssetCode,
		c.AssetDescription,
		c.CostCenter,
		c.CostCenterDescription,
		c.CostUnit,
		c.CostUnitDescription,
		bigquerytools.DateToNullTimestamp(c.Created),
		c.Creator.String(),
		c.CreatorFullName,
		bigquerytools.DateToNullTimestamp(c.Date),
		c.Description,
		c.Division,
		c.Document.String(),
		c.DocumentNumber,
		c.DocumentSubject,
		c.EntryID.String(),
		c.EntryNumber,
		c.ExchangeRate,
		c.GLAccount.String(),
		c.GLAccountCode,
		c.GLAccountDescription,
		c.LineNumber,
		bigquerytools.DateToNullTimestamp(c.Modified),
		c.Modifier.String(),
		c.ModifierFullName,
		c.Notes,
		c.OffsetID.String(),
		c.OurRef,
		c.Project.String(),
		c.ProjectCode,
		c.ProjectDescription,
		c.Quantity,
		c.VATCode,
		c.VATCodeDescription,
		c.VATPercentage,
		c.VATType,
	}
}

func (client *Client) GetBankEntryLinesBQ(lastModified *time.Time) (*[]BankEntryLineBQ, error) {
	gds, err := client.ExactOnline().FinancialTransactionClient.GetBankEntryLines(lastModified)
	if err != nil {
		return nil, err
	}

	if gds == nil {
		return nil, nil
	}

	rowCount := len(*gds)

	fmt.Printf("#BankEntryLines for client %s: %v\n", client.ClientID(), rowCount)

	gdsBQ := []BankEntryLineBQ{}

	for _, gd := range *gds {
		gdsBQ = append(gdsBQ, getBankEntryLineBQ(&gd, client.ClientID()))
	}

	return &gdsBQ, nil
}

func (client *Client) WriteBankEntryLinesBQ(writeToObject *storage.ObjectHandle, lastModified *time.Time) (int, interface{}, error) {
	if writeToObject == nil {
		return 0, nil, nil
	}

	gdsBQ, err := client.GetBankEntryLinesBQ(lastModified)
	if err != nil {
		return 0, nil, err
	}

	if gdsBQ == nil {
		return 0, nil, nil
	}

	ctx := context.Background()

	w := writeToObject.NewWriter(ctx)

	for _, gdBQ := range *gdsBQ {

		b, err := json.Marshal(gdBQ)
		if err != nil {
			return 0, nil, err
		}

		// Write data
		_, err = w.Write(b)
		if err != nil {
			return 0, nil, err
		}

		// Write NewLine
		_, err = fmt.Fprintf(w, "\n")
		if err != nil {
			return 0, nil, err
		}
	}

	// Close
	err = w.Close()
	if err != nil {
		return 0, nil, err
	}

	return len(*gdsBQ), BankEntryLineBQ{}, nil
}
