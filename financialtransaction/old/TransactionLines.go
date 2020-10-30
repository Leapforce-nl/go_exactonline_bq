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

type TransactionLineBQ struct {
	ClientID                  string
	ID                        string
	Account                   string
	AccountCode               string
	AccountName               string
	AmountDC                  float64
	AmountFC                  float64
	AmountVATBaseFC           float64
	AmountVATFC               float64
	Asset                     string
	AssetCode                 string
	AssetDescription          string
	CostCenter                string
	CostCenterDescription     string
	CostUnit                  string
	CostUnitDescription       string
	Created                   bigquery.NullTimestamp
	Creator                   string
	CreatorFullName           string
	Currency                  string
	Date                      bigquery.NullTimestamp
	Description               string
	Division                  int64
	Document                  string
	DocumentNumber            int64
	DocumentSubject           string
	DueDate                   bigquery.NullTimestamp
	EntryID                   string
	EntryNumber               int64
	ExchangeRate              float64
	ExtraDutyAmountFC         float64
	ExtraDutyPercentage       float64
	FinancialPeriod           int64
	FinancialYear             int64
	GLAccount                 string
	GLAccountCode             string
	GLAccountDescription      string
	InvoiceNumber             int64
	Item                      string
	ItemCode                  string
	ItemDescription           string
	JournalCode               string
	JournalDescription        string
	LineNumber                int64
	LineType                  int64
	Modified                  bigquery.NullTimestamp
	Modifier                  string
	ModifierFullName          string
	Notes                     string
	OffsetID                  string
	OrderNumber               int64
	PaymentDiscountAmount     float64
	PaymentReference          string
	Project                   string
	ProjectCode               string
	ProjectDescription        string
	Quantity                  float64
	SerialNumber              string
	Status                    int64
	Subscription              string
	SubscriptionDescription   string
	TrackingNumber            string
	TrackingNumberDescription string
	Type                      int64
	VATCode                   string
	VATCodeDescription        string
	VATPercentage             float64
	VATType                   string
	YourRef                   string
}

func getTransactionLineBQ(c *financialtransaction.TransactionLine, clientID string) TransactionLineBQ {
	return TransactionLineBQ{
		clientID,
		c.ID.String(),
		c.Account.String(),
		c.AccountCode,
		c.AccountName,
		c.AmountDC,
		c.AmountFC,
		c.AmountVATBaseFC,
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
		c.Currency,
		bigquerytools.DateToNullTimestamp(c.Date),
		c.Description,
		c.Division,
		c.Document.String(),
		c.DocumentNumber,
		c.DocumentSubject,
		bigquerytools.DateToNullTimestamp(c.DueDate),
		c.EntryID.String(),
		c.EntryNumber,
		c.ExchangeRate,
		c.ExtraDutyAmountFC,
		c.ExtraDutyPercentage,
		c.FinancialPeriod,
		c.FinancialYear,
		c.GLAccount.String(),
		c.GLAccountCode,
		c.GLAccountDescription,
		c.InvoiceNumber,
		c.Item.String(),
		c.ItemCode,
		c.ItemDescription,
		c.JournalCode,
		c.JournalDescription,
		c.LineNumber,
		c.LineType,
		bigquerytools.DateToNullTimestamp(c.Modified),
		c.Modifier.String(),
		c.ModifierFullName,
		c.Notes,
		c.OffsetID.String(),
		c.OrderNumber,
		c.PaymentDiscountAmount,
		c.PaymentReference,
		c.Project.String(),
		c.ProjectCode,
		c.ProjectDescription,
		c.Quantity,
		c.SerialNumber,
		c.Status,
		c.Subscription.String(),
		c.SubscriptionDescription,
		c.TrackingNumber,
		c.TrackingNumberDescription,
		c.Type,
		c.VATCode,
		c.VATCodeDescription,
		c.VATPercentage,
		c.VATType,
		c.YourRef,
	}
}

func (client *Client) GetTransactionLinesBQ(lastModified *time.Time) (*[]TransactionLineBQ, error) {
	gds, err := client.ExactOnline().FinancialTransactionClient.GetTransactionLines(lastModified)
	if err != nil {
		return nil, err
	}

	if gds == nil {
		return nil, nil
	}

	rowCount := len(*gds)

	fmt.Printf("#TransactionLines for client %s: %v\n", client.ClientID(), rowCount)

	gdsBQ := []TransactionLineBQ{}

	for _, gd := range *gds {
		gdsBQ = append(gdsBQ, getTransactionLineBQ(&gd, client.ClientID()))
	}

	return &gdsBQ, nil
}

func (client *Client) WriteTransactionLinesBQ(writeToObject *storage.ObjectHandle, lastModified *time.Time) (int, interface{}, error) {
	if writeToObject == nil {
		return 0, nil, nil
	}

	gdsBQ, err := client.GetTransactionLinesBQ(lastModified)
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

	return len(*gdsBQ), TransactionLineBQ{}, nil
}
