package exactonline_bq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"

	errortools "github.com/leapforce-libraries/go_errortools"
	financialtransaction "github.com/leapforce-libraries/go_exactonline_new/financialtransaction"
	google "github.com/leapforce-libraries/go_google"
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
		google.DateToNullTimestamp(c.Created),
		c.Creator.String(),
		c.CreatorFullName,
		google.DateToNullTimestamp(c.Date),
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
		google.DateToNullTimestamp(c.Modified),
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

func (service *Service) WriteBankEntryLinesBQ(bucketHandle *storage.BucketHandle, lastModified *time.Time) ([]*storage.ObjectHandle, int, interface{}, *errortools.Error) {
	if bucketHandle == nil {
		return nil, 0, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	call := service.FinancialTransactionService().NewGetBankEntryLinesCall(lastModified)

	rowCount := 0
	batchRowCount := 0
	batchSize := 10000

	for true {
		bankEntryLines, e := call.Do()
		if e != nil {
			return nil, 0, nil, e
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

			b, err := json.Marshal(getBankEntryLineBQ(&tl, service.ClientID()))
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

			fmt.Printf("#BankEntryLines for service %s flushed: %v\n", service.ClientID(), batchRowCount)

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

	fmt.Printf("#BankEntryLines for service %s: %v\n", service.ClientID(), rowCount)

	return objectHandles, rowCount, BankEntryLineBQ{}, nil
}
