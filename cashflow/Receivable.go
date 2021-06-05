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

type Receivable struct {
	OrganisationID_               int64
	SoftwareClientLicenceID_      int64
	ID                            string
	Account                       string
	AccountBankAccountID          string
	AccountBankAccountNumber      string
	AccountCode                   string
	AccountContact                string
	AccountContactName            string
	AccountCountry                string
	AccountName                   string
	AmountDC                      float64
	AmountDiscountDC              float64
	AmountDiscountFC              float64
	AmountFC                      float64
	BankAccountID                 string
	BankAccountNumber             string
	CashflowTransactionBatchCode  string
	Created                       bigquery.NullTimestamp
	Creator                       string
	CreatorFullName               string
	Currency                      string
	Description                   string
	DirectDebitMandate            string
	DirectDebitMandateDescription string
	DirectDebitMandatePaymentType int16
	DirectDebitMandateReference   string
	DirectDebitMandateType        int16
	DiscountDueDate               bigquery.NullTimestamp
	Division                      int32
	Document                      string
	DocumentNumber                int32
	DocumentSubject               string
	DueDate                       bigquery.NullTimestamp
	EndDate                       bigquery.NullTimestamp
	EndPeriod                     int16
	EndToEndID                    string
	EndYear                       int16
	EntryDate                     bigquery.NullTimestamp
	EntryID                       string
	EntryNumber                   int32
	GLAccount                     string
	GLAccountCode                 string
	GLAccountDescription          string
	InvoiceDate                   bigquery.NullTimestamp
	InvoiceNumber                 int32
	IsBatchBooking                byte
	IsFullyPaid                   bool
	Journal                       string
	JournalDescription            string
	LastPaymentDate               bigquery.NullTimestamp
	Modified                      bigquery.NullTimestamp
	Modifier                      string
	ModifierFullName              string
	PaymentCondition              string
	PaymentConditionDescription   string
	PaymentDays                   int32
	PaymentDaysDiscount           int32
	PaymentDiscountPercentage     float64
	PaymentInformationID          string
	PaymentMethod                 string
	PaymentReference              string
	RateFC                        float64
	ReceivableBatchNumber         int32
	ReceivableSelected            bigquery.NullTimestamp
	ReceivableSelector            string
	ReceivableSelectorFullName    string
	Source                        int32
	Status                        int16
	TransactionAmountDC           float64
	TransactionAmountFC           float64
	TransactionDueDate            bigquery.NullTimestamp
	TransactionEntryID            string
	TransactionID                 string
	TransactionIsReversal         bool
	TransactionReportingPeriod    int16
	TransactionReportingYear      int16
	TransactionStatus             int16
	TransactionType               int32
	YourRef                       string
}

func getReceivable(c *cashflow.Receivable, organisationID int64, softwareClientLicenceID int64) Receivable {
	return Receivable{
		organisationID,
		softwareClientLicenceID,
		c.ID.String(),
		c.Account.String(),
		c.AccountBankAccountID.String(),
		c.AccountBankAccountNumber,
		c.AccountCode,
		c.AccountContact.String(),
		c.AccountContactName,
		c.AccountCountry,
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
		c.DirectDebitMandate.String(),
		c.DirectDebitMandateDescription,
		c.DirectDebitMandatePaymentType,
		c.DirectDebitMandateReference,
		c.DirectDebitMandateType,
		go_bigquery.DateToNullTimestamp(c.DiscountDueDate),
		c.Division,
		c.Document.String(),
		c.DocumentNumber,
		c.DocumentSubject,
		go_bigquery.DateToNullTimestamp(c.DueDate),
		go_bigquery.DateToNullTimestamp(c.EndDate),
		c.EndPeriod,
		c.EndToEndID,
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
		c.IsFullyPaid,
		c.Journal,
		c.JournalDescription,
		go_bigquery.DateToNullTimestamp(c.LastPaymentDate),
		go_bigquery.DateToNullTimestamp(c.Modified),
		c.Modifier.String(),
		c.ModifierFullName,
		c.PaymentCondition,
		c.PaymentConditionDescription,
		c.PaymentDays,
		c.PaymentDaysDiscount,
		c.PaymentDiscountPercentage,
		c.PaymentInformationID,
		c.PaymentMethod,
		c.PaymentReference,
		c.RateFC,
		c.ReceivableBatchNumber,
		go_bigquery.DateToNullTimestamp(c.ReceivableSelected),
		c.ReceivableSelector.String(),
		c.ReceivableSelectorFullName,
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

func (service *Service) WriteReceivablesBQ(bucketHandle *storage.BucketHandle, organisationID int64, softwareClientLicenceID int64, lastModified *time.Time) ([]*storage.ObjectHandle, int, interface{}, *errortools.Error) {
	if bucketHandle == nil {
		return nil, 0, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	call := service.CashflowService().NewGetReceivablesCall(lastModified)

	rowCount := 0
	batchRowCount := 0
	batchSize := 10000

	for true {
		receivables, e := call.Do()
		if e != nil {
			return nil, 0, nil, e
		}

		if receivables == nil {
			break
		}

		if batchRowCount == 0 {
			guid := types.NewGUID()
			objectHandle := bucketHandle.Object((&guid).String())
			objectHandles = append(objectHandles, objectHandle)

			w = objectHandle.NewWriter(context.Background())
		}

		for _, tl := range *receivables {
			batchRowCount++

			b, err := json.Marshal(getReceivable(&tl, organisationID, softwareClientLicenceID))
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

			fmt.Printf("#Receivables for flushed: %v\n", batchRowCount)

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

	fmt.Printf("#Receivables: %v\n", rowCount)

	return objectHandles, rowCount, Receivable{}, nil
}
