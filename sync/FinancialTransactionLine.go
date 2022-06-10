package exactonline_bq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	bigquery "cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	errortools "github.com/leapforce-libraries/go_errortools"
	sync "github.com/leapforce-libraries/go_exactonline_new/sync"
	go_bigquery "github.com/leapforce-libraries/go_google/bigquery"
	types "github.com/leapforce-libraries/go_types"
)

type FinancialTransactionLine struct {
	OrganisationID_           int64
	SoftwareClientLicenceID_  int64
	Created_                  time.Time
	Modified_                 time.Time
	Timestamp                 int64
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

func getFinancialTransactionLine(c *sync.FinancialTransactionLine, organisationID int64, softwareClientLicenceID int64, maxTimestamp *int64) FinancialTransactionLine {
	timestamp := c.Timestamp.Value()
	if timestamp > *maxTimestamp {
		*maxTimestamp = timestamp
	}

	t := time.Now()

	return FinancialTransactionLine{
		organisationID,
		softwareClientLicenceID,
		t, t,
		c.Timestamp.Value(),
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
		go_bigquery.DateToNullTimestamp(c.Created),
		c.Creator.String(),
		c.CreatorFullName,
		c.Currency,
		go_bigquery.DateToNullTimestamp(c.Date),
		c.Description,
		c.Division,
		c.Document.String(),
		c.DocumentNumber,
		c.DocumentSubject,
		go_bigquery.DateToNullTimestamp(c.DueDate),
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
		go_bigquery.DateToNullTimestamp(c.Modified),
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

func (service *Service) WriteFinancialTransactionLines(bucketHandle *storage.BucketHandle, organisationID int64, softwareClientLicenceID int64, timestamp int64) ([]*storage.ObjectHandle, *int64, *errortools.Error) {
	if bucketHandle == nil {
		return nil, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	call := service.SyncService().NewSyncFinancialTransactionLinesCall(&timestamp)

	rowCount := 0
	batchRowCount := 0
	batchSize := 10000

	maxTimestamp := int64(0)

	for {
		transactionLines, e := call.Do()
		if e != nil {
			return nil, nil, e
		}

		if transactionLines == nil {
			break
		}

		if batchRowCount == 0 {
			guid := types.NewGuid()
			objectHandle := bucketHandle.Object((&guid).String())
			objectHandles = append(objectHandles, objectHandle)

			w = objectHandle.NewWriter(context.Background())
		}

		for _, tl := range *transactionLines {
			batchRowCount++

			b, err := json.Marshal(getFinancialTransactionLine(&tl, organisationID, softwareClientLicenceID, &maxTimestamp))
			if err != nil {
				return nil, nil, errortools.ErrorMessage(err)
			}

			// Write data
			_, err = w.Write(b)
			if err != nil {
				return nil, nil, errortools.ErrorMessage(err)
			}

			// Write NewLine
			_, err = fmt.Fprintf(w, "\n")
			if err != nil {
				return nil, nil, errortools.ErrorMessage(err)
			}
		}

		if batchRowCount > batchSize {
			// Close and flush data
			err := w.Close()
			if err != nil {
				return nil, nil, errortools.ErrorMessage(err)
			}
			w = nil

			fmt.Printf("#FinancialTransactionLines flushed: %v\n", batchRowCount)

			rowCount += batchRowCount
			batchRowCount = 0
		}
	}

	if w != nil {
		// Close and flush data
		err := w.Close()
		if err != nil {
			return nil, nil, errortools.ErrorMessage(err)
		}

		rowCount += batchRowCount
	}

	fmt.Printf("#FinancialTransactionLines: %v\n", rowCount)

	return objectHandles, &maxTimestamp, nil
}
