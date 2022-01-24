package exactonline_bq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"

	errortools "github.com/leapforce-libraries/go_errortools"
	cashflow "github.com/leapforce-libraries/go_exactonline_new/cashflow"
	go_bigquery "github.com/leapforce-libraries/go_google/bigquery"
	types "github.com/leapforce-libraries/go_types"
)

type Payment struct {
	OrganisationID_              int64
	SoftwareClientLicenceID_     int64
	Created_                     time.Time
	Modified_                    time.Time
	ID                           string
	Account                      string
	AccountBankAccountID         string
	AccountBankAccountNumber     string
	AccountCode                  string
	AccountContact               string
	AccountContactName           string
	AccountName                  string
	AmountDC                     float64
	AmountDiscountDC             float64
	AmountDiscountFC             float64
	AmountFC                     float64
	BankAccountID                string
	BankAccountNumber            string
	CashflowTransactionBatchCode string
	Created                      bigquery.NullTimestamp
	Creator                      string
	CreatorFullName              string
	Currency                     string
	Description                  string
	DiscountDueDate              bigquery.NullTimestamp
	Division                     int32
	Document                     string
	DocumentNumber               int32
	DocumentSubject              string
	DueDate                      bigquery.NullTimestamp
	EndDate                      bigquery.NullTimestamp
	EndPeriod                    int16
	EndYear                      int16
	EntryDate                    bigquery.NullTimestamp
	EntryID                      string
	EntryNumber                  int32
	GLAccount                    string
	GLAccountCode                string
	GLAccountDescription         string
	InvoiceDate                  bigquery.NullTimestamp
	InvoiceNumber                int32
	IsBatchBooking               byte
	Journal                      string
	JournalDescription           string
	Modified                     bigquery.NullTimestamp
	Modifier                     string
	ModifierFullName             string
	PaymentBatchNumber           int32
	PaymentCondition             string
	PaymentConditionDescription  string
	PaymentDays                  int32
	PaymentDaysDiscount          int32
	PaymentDiscountPercentage    float64
	PaymentMethod                string
	PaymentReference             string
	PaymentSelected              bigquery.NullTimestamp
	PaymentSelector              string
	PaymentSelectorFullName      string
	RateFC                       float64
	Source                       int32
	Status                       int16
	TransactionAmountDC          float64
	TransactionAmountFC          float64
	TransactionDueDate           bigquery.NullTimestamp
	TransactionEntryID           string
	TransactionID                string
	TransactionIsReversal        bool
	TransactionReportingPeriod   int16
	TransactionReportingYear     int16
	TransactionStatus            int16
	TransactionType              int32
	YourRef                      string
}

func getPayment(c *cashflow.Payment, organisationID int64, softwareClientLicenceID int64) Payment {
	t := time.Now()

	return Payment{
		organisationID,
		softwareClientLicenceID,
		t, t,
		c.ID.String(),
		c.Account.String(),
		c.AccountBankAccountID.String(),
		c.AccountBankAccountNumber,
		c.AccountCode,
		c.AccountContact.String(),
		c.AccountContactName,
		c.AccountName,
		c.AmountDC,
		c.AmountDiscountDC,
		c.AmountDiscountFC,
		c.AmountFC,
		c.BankAccountID.String(),
		c.BankAccountNumber,
		c.CashflowTransactionBatchCode,
		go_bigquery.DateToNullTimestamp(c.Created),
		c.Creator.String(),
		c.CreatorFullName,
		c.Currency,
		c.Description,
		go_bigquery.DateToNullTimestamp(c.DiscountDueDate),
		c.Division,
		c.Document.String(),
		c.DocumentNumber,
		c.DocumentSubject,
		go_bigquery.DateToNullTimestamp(c.DueDate),
		go_bigquery.DateToNullTimestamp(c.EndDate),
		c.EndPeriod,
		c.EndYear,
		go_bigquery.DateToNullTimestamp(c.EntryDate),
		c.EntryID.String(),
		c.EntryNumber,
		c.GLAccount.String(),
		c.GLAccountCode,
		c.GLAccountDescription,
		go_bigquery.DateToNullTimestamp(c.InvoiceDate),
		c.InvoiceNumber,
		c.IsBatchBooking,
		c.Journal,
		c.JournalDescription,
		go_bigquery.DateToNullTimestamp(c.Modified),
		c.Modifier.String(),
		c.ModifierFullName,
		c.PaymentBatchNumber,
		c.PaymentCondition,
		c.PaymentConditionDescription,
		c.PaymentDays,
		c.PaymentDaysDiscount,
		c.PaymentDiscountPercentage,
		c.PaymentMethod,
		c.PaymentReference,
		go_bigquery.DateToNullTimestamp(c.PaymentSelected),
		c.PaymentSelector.String(),
		c.PaymentSelectorFullName,
		c.RateFC,
		c.Source,
		c.Status,
		c.TransactionAmountDC,
		c.TransactionAmountFC,
		go_bigquery.DateToNullTimestamp(c.TransactionDueDate),
		c.TransactionEntryID.String(),
		c.TransactionID.String(),
		c.TransactionIsReversal,
		c.TransactionReportingPeriod,
		c.TransactionReportingYear,
		c.TransactionStatus,
		c.TransactionType,
		c.YourRef,
	}
}

func (service *Service) WritePaymentsBQ(bucketHandle *storage.BucketHandle, organisationID int64, softwareClientLicenceID int64, lastModified *time.Time) ([]*storage.ObjectHandle, int, interface{}, *errortools.Error) {
	if bucketHandle == nil {
		return nil, 0, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	call := service.CashflowService().NewGetPaymentsCall(lastModified)

	rowCount := 0
	batchRowCount := 0
	batchSize := 10000

	for true {
		payments, e := call.Do()
		if e != nil {
			return nil, 0, nil, e
		}

		if payments == nil {
			break
		}

		if batchRowCount == 0 {
			guid := types.NewGUID()
			objectHandle := bucketHandle.Object((&guid).String())
			objectHandles = append(objectHandles, objectHandle)

			w = objectHandle.NewWriter(context.Background())
		}

		for _, tl := range *payments {
			batchRowCount++

			b, err := json.Marshal(getPayment(&tl, organisationID, softwareClientLicenceID))
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

			fmt.Printf("#Payments flushed: %v\n", batchRowCount)

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

	fmt.Printf("#Payments: %v\n", rowCount)

	return objectHandles, rowCount, Payment{}, nil
}
