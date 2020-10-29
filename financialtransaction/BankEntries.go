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

func (client *Client) GetBankEntriesBQ(lastModified *time.Time) (*[]BankEntryBQ, error) {
	gds, err := client.ExactOnline().FinancialTransactionClient.GetBankEntries(lastModified)
	if err != nil {
		return nil, err
	}

	if gds == nil {
		return nil, nil
	}

	rowCount := len(*gds)

	fmt.Printf("#BankEntries for client %s: %v\n", client.ClientID(), rowCount)

	gdsBQ := []BankEntryBQ{}

	for _, gd := range *gds {
		gdsBQ = append(gdsBQ, getBankEntryBQ(&gd, client.ClientID()))
	}

	return &gdsBQ, nil
}

func (client *Client) WriteBankEntriesBQ(writeToObject *storage.ObjectHandle, lastModified *time.Time) (int, interface{}, error) {
	if writeToObject == nil {
		return 0, nil, nil
	}

	gdsBQ, err := client.GetBankEntriesBQ(lastModified)
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

	return len(*gdsBQ), BankEntryBQ{}, nil
}
