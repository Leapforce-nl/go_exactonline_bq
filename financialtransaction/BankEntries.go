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
	types "github.com/Leapforce-nl/go_types"
)

type BankEntryBQ struct {
	ClientID string
	EntryID  string
	//BankEntryLines
	BankStatementDocument        string
	BankStatementDocumentNumber  int32
	BankStatementDocumentSubject string
	ClosingBalanceFC             float64
	Created                      bigquery.NullTimestamp
	Currency                     string
	Division                     int32
	EntryNumber                  int32
	FinancialPeriod              int16
	FinancialYear                int16
	JournalCode                  string
	JournalDescription           string
	Modified                     bigquery.NullTimestamp
	OpeningBalanceFC             float64
	Status                       int16
	StatusDescription            string
}

func getBankEntryBQ(c *financialtransaction.BankEntry, clientID string) BankEntryBQ {
	return BankEntryBQ{
		clientID,
		c.EntryID.String(),
		//c.BankEntryLines,
		c.BankStatementDocument.String(),
		c.BankStatementDocumentNumber,
		c.BankStatementDocumentSubject,
		c.ClosingBalanceFC,
		bigquerytools.DateToNullTimestamp(c.Created),
		c.Currency,
		c.Division,
		c.EntryNumber,
		c.FinancialPeriod,
		c.FinancialYear,
		c.JournalCode,
		c.JournalDescription,
		bigquerytools.DateToNullTimestamp(c.Modified),
		c.OpeningBalanceFC,
		c.Status,
		c.StatusDescription,
	}
}

func (client *Client) WriteBankEntriesBQ(bucketHandle *storage.BucketHandle, lastModified *time.Time) ([]*storage.ObjectHandle, int, interface{}, error) {
	if bucketHandle == nil {
		return nil, 0, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	call := client.ExactOnline().FinancialTransactionClient.NewGetBankEntriesCall(lastModified)

	rowCount := 0
	batchRowCount := 0
	batchSize := 10000

	for true {
		bankEntries, err := call.Do()
		if err != nil {
			return nil, 0, nil, err
		}

		if bankEntries == nil {
			break
		}

		if batchRowCount == 0 {
			guid := types.NewGUID()
			objectHandle := bucketHandle.Object((&guid).String())
			objectHandles = append(objectHandles, objectHandle)

			w = objectHandle.NewWriter(context.Background())
		}

		for _, tl := range *bankEntries {
			batchRowCount++

			b, err := json.Marshal(getBankEntryBQ(&tl, client.ClientID()))
			if err != nil {
				return nil, 0, nil, err
			}

			// Write data
			_, err = w.Write(b)
			if err != nil {
				return nil, 0, nil, err
			}

			// Write NewLine
			_, err = fmt.Fprintf(w, "\n")
			if err != nil {
				return nil, 0, nil, err
			}
		}

		if batchRowCount > batchSize {
			// Close and flush data
			err = w.Close()
			if err != nil {
				return nil, 0, nil, err
			}
			w = nil

			fmt.Printf("#BankEntries for client %s flushed: %v\n", client.ClientID(), batchRowCount)

			rowCount += batchRowCount
			batchRowCount = 0
		}
	}

	if w != nil {
		// Close and flush data
		err := w.Close()
		if err != nil {
			return nil, 0, nil, err
		}

		rowCount += batchRowCount
	}

	fmt.Printf("#BankEntries for client %s: %v\n", client.ClientID(), rowCount)

	return objectHandles, rowCount, BankEntryBQ{}, nil
}
