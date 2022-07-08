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

type BankEntry struct {
	SoftwareClientLicenseGuid_ string
	Created_                   time.Time
	Modified_                  time.Time
	EntryID                    string
	//BankEntryLines
	BankStatementDocument        string
	BankStatementDocumentNumber  int32
	BankStatementDocumentSubject string
	ClosingBalanceFC             float64
	Created                      _bigquery.NullTimestamp
	Currency                     string
	Division                     int32
	EntryNumber                  int32
	FinancialPeriod              int16
	FinancialYear                int16
	JournalCode                  string
	JournalDescription           string
	Modified                     _bigquery.NullTimestamp
	OpeningBalanceFC             float64
	Status                       int16
	StatusDescription            string
}

func getBankEntry(c *financialtransaction.BankEntry, softwareClientLicenseGuid string) BankEntry {
	t := time.Now()

	return BankEntry{
		softwareClientLicenseGuid,
		t, t,
		c.EntryID.String(),
		//c.BankEntryLines,
		c.BankStatementDocument.String(),
		c.BankStatementDocumentNumber,
		c.BankStatementDocumentSubject,
		c.ClosingBalanceFC,
		bigquery.DateToNullTimestamp(c.Created),
		c.Currency,
		c.Division,
		c.EntryNumber,
		c.FinancialPeriod,
		c.FinancialYear,
		c.JournalCode,
		c.JournalDescription,
		bigquery.DateToNullTimestamp(c.Modified),
		c.OpeningBalanceFC,
		c.Status,
		c.StatusDescription,
	}
}

func (service *Service) WriteBankEntries(bucketHandle *storage.BucketHandle, softwareClientLicenseGuid string, lastModified *time.Time) ([]*storage.ObjectHandle, int, interface{}, *errortools.Error) {
	if bucketHandle == nil {
		return nil, 0, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	call := service.FinancialTransactionService().NewGetBankEntriesCall(lastModified)

	rowCount := 0
	batchRowCount := 0
	batchSize := 10000

	for true {
		bankEntries, e := call.Do()
		if e != nil {
			return nil, 0, nil, e
		}

		if bankEntries == nil {
			break
		}

		if batchRowCount == 0 {
			guid := types.NewGuid()
			objectHandle := bucketHandle.Object((&guid).String())
			objectHandles = append(objectHandles, objectHandle)

			w = objectHandle.NewWriter(context.Background())
		}

		for _, tl := range *bankEntries {
			batchRowCount++

			b, err := json.Marshal(getBankEntry(&tl, softwareClientLicenseGuid))
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

			fmt.Printf("#BankEntries flushed: %v\n", batchRowCount)

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

	fmt.Printf("#BankEntries: %v\n", rowCount)

	return objectHandles, rowCount, BankEntry{}, nil
}
