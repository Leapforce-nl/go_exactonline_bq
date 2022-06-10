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

type FinancialGLAccount struct {
	OrganisationID_                int64
	SoftwareClientLicenceID_       int64
	Created_                       time.Time
	Modified_                      time.Time
	Timestamp                      int64
	AssimilatedVATBox              int16
	BalanceSide                    string
	BalanceType                    string
	BelcotaxType                   int32
	Code                           string
	Compress                       bool
	Costcenter                     string
	CostcenterDescription          string
	Costunit                       string
	CostunitDescription            string
	Created                        bigquery.NullTimestamp
	Creator                        string
	CreatorFullName                string
	Description                    string
	Division                       int32
	ExcludeVATListing              byte
	ExpenseNonDeductiblePercentage float64
	ID                             string
	IsBlocked                      bool
	Matching                       bool
	Modified                       bigquery.NullTimestamp
	Modifier                       string
	ModifierFullName               string
	PrivateGLAccount               string
	PrivatePercentage              float64
	ReportingCode                  string
	RevalueCurrency                bool
	SearchCode                     string
	Type                           int32
	TypeDescription                string
	UseCostcenter                  byte
	UseCostunit                    byte
	VATCode                        string
	VATDescription                 string
	VATGLAccountType               string
	VATNonDeductibleGLAccount      string
	VATNonDeductiblePercentage     float64
	VATSystem                      string
	YearEndCostGLAccount           string
	YearEndReflectionGLAccount     string
}

func getFinancialGLAccount(c *sync.FinancialGLAccount, organisationID int64, softwareClientLicenceID int64, maxTimestamp *int64) FinancialGLAccount {
	timestamp := c.Timestamp.Value()
	if timestamp > *maxTimestamp {
		*maxTimestamp = timestamp
	}

	t := time.Now()

	return FinancialGLAccount{
		organisationID,
		softwareClientLicenceID,
		t, t,
		c.Timestamp.Value(),
		c.AssimilatedVATBox,
		c.BalanceSide,
		c.BalanceType,
		c.BelcotaxType,
		c.Code,
		c.Compress,
		c.Costcenter,
		c.CostcenterDescription,
		c.Costunit,
		c.CostunitDescription,
		go_bigquery.DateToNullTimestamp(c.Created),
		c.Creator.String(),
		c.CreatorFullName,
		c.Description,
		c.Division,
		c.ExcludeVATListing,
		c.ExpenseNonDeductiblePercentage,
		c.ID.String(),
		c.IsBlocked,
		c.Matching,
		go_bigquery.DateToNullTimestamp(c.Modified),
		c.Modifier.String(),
		c.ModifierFullName,
		c.PrivateGLAccount.String(),
		c.PrivatePercentage,
		c.ReportingCode,
		c.RevalueCurrency,
		c.SearchCode,
		c.Type,
		c.TypeDescription,
		c.UseCostcenter,
		c.UseCostunit,
		c.VATCode,
		c.VATDescription,
		c.VATGLAccountType,
		c.VATNonDeductibleGLAccount.String(),
		c.VATNonDeductiblePercentage,
		c.VATSystem,
		c.YearEndCostGLAccount.String(),
		c.YearEndReflectionGLAccount.String(),
	}
}

func (service *Service) WriteFinancialGLAccounts(bucketHandle *storage.BucketHandle, organisationID int64, softwareClientLicenceID int64, timestamp int64) ([]*storage.ObjectHandle, *int64, *errortools.Error) {
	if bucketHandle == nil {
		return nil, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	call := service.SyncService().NewSyncFinancialGLAccountsCall(&timestamp)

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

			b, err := json.Marshal(getFinancialGLAccount(&tl, organisationID, softwareClientLicenceID, &maxTimestamp))
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

			fmt.Printf("#FinancialGLAccounts flushed: %v\n", batchRowCount)

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

	fmt.Printf("#FinancialGLAccounts: %v\n", rowCount)

	return objectHandles, &maxTimestamp, nil
}
