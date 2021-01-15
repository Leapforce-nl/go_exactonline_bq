package exactonline_bq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	_bigquery "cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"

	errortools "github.com/leapforce-libraries/go_errortools"
	financialtransaction "github.com/leapforce-libraries/go_exactonline_new/financialtransaction"
	bigquery "github.com/leapforce-libraries/go_google/bigquery"
	types "github.com/leapforce-libraries/go_types"
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
	Created                   _bigquery.NullTimestamp
	Creator                   string
	CreatorFullName           string
	Currency                  string
	Date                      _bigquery.NullTimestamp
	Description               string
	Division                  int64
	Document                  string
	DocumentNumber            int64
	DocumentSubject           string
	DueDate                   _bigquery.NullTimestamp
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
	Modified                  _bigquery.NullTimestamp
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
		bigquery.DateToNullTimestamp(c.Created),
		c.Creator.String(),
		c.CreatorFullName,
		c.Currency,
		bigquery.DateToNullTimestamp(c.Date),
		c.Description,
		c.Division,
		c.Document.String(),
		c.DocumentNumber,
		c.DocumentSubject,
		bigquery.DateToNullTimestamp(c.DueDate),
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
		bigquery.DateToNullTimestamp(c.Modified),
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

func (service *Service) WriteTransactionLinesBQ(bucketHandle *storage.BucketHandle, lastModified *time.Time) ([]*storage.ObjectHandle, int, interface{}, *errortools.Error) {
	if bucketHandle == nil {
		return nil, 0, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	call := service.FinancialTransactionService().NewGetTransactionLinesCall(lastModified)

	rowCount := 0
	batchRowCount := 0
	batchSize := 10000

	for true {
		transactionLines, e := call.Do()
		if e != nil {
			return nil, 0, nil, e
		}

		if transactionLines == nil {
			break
		}

		if batchRowCount == 0 {
			guid := types.NewGUID()
			objectHandle := bucketHandle.Object((&guid).String())
			objectHandles = append(objectHandles, objectHandle)

			w = objectHandle.NewWriter(context.Background())
		}

		for _, tl := range *transactionLines {
			batchRowCount++

			b, err := json.Marshal(getTransactionLineBQ(&tl, service.ClientID()))
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

			fmt.Printf("#TransactionLines for service %s flushed: %v\n", service.ClientID(), batchRowCount)

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

	fmt.Printf("#TransactionLines for service %s: %v\n", service.ClientID(), rowCount)

	return objectHandles, rowCount, TransactionLineBQ{}, nil
}
