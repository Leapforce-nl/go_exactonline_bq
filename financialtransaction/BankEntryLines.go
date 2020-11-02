package exactonline_bq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"

	bigquerytools "github.com/leapforce-libraries/go_bigquerytools"
	financialtransaction "github.com/leapforce-libraries/go_exactonline_new/financialtransaction"
	types "github.com/leapforce-libraries/go_types"
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

func (client *Client) WriteBankEntryLinesBQ(bucketHandle *storage.BucketHandle, lastModified *time.Time) ([]*storage.ObjectHandle, int, interface{}, error) {
	if bucketHandle == nil {
		return nil, 0, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	call := client.ExactOnline().FinancialTransactionClient.NewGetBankEntryLinesCall(lastModified)

	rowCount := 0
	batchRowCount := 0
	batchSize := 10000

	for true {
		bankEntryLines, err := call.Do()
		if err != nil {
			return nil, 0, nil, err
		}

		if bankEntryLines == nil {
			break
		}

		if batchRowCount == 0 {
			guid := types.NewGUID()
			objectHandle := bucketHandle.Object((&guid).String())
			objectHandles = append(objectHandles, objectHandle)

			w = objectHandle.NewWriter(context.Background())
		}

		for _, tl := range *bankEntryLines {
			batchRowCount++

			b, err := json.Marshal(getBankEntryLineBQ(&tl, client.ClientID()))
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

			fmt.Printf("#BankEntryLines for client %s flushed: %v\n", client.ClientID(), batchRowCount)

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

	fmt.Printf("#BankEntryLines for client %s: %v\n", client.ClientID(), rowCount)

	return objectHandles, rowCount, BankEntryLineBQ{}, nil
}
